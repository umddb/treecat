import json
import random
import pandas as pd
from datetime import date, timedelta

NUM_FILES = 50

def genString(length):
    final_length = random.randint(1, length)
    output_str = ""
    for i in range(final_length):
        output_str = output_str + chr(random.randint(1, 26) + 64)
    return output_str

def genInt(cardinality):
    return random.randint(1, cardinality)

def intToStr(length, integer):
    output_str = ""
    quotient = integer
    cur_length = 0
    while quotient > 0:
        remainder = quotient % 26
        quotient = quotient // 26
        output_str = chr(remainder + 65) + output_str
        cur_length += 1
    # if the string is not of the desirable length, pad with A
    if (length - cur_length) > 0: 
        output_str = ("A" * (length - cur_length)) + output_str
    return output_str

def getTable(tables, table_name):
    for table in tables:
        if table["name"] == table_name:
            return table 
    return None

def genDecimal(num_digits):
    return random.randint(0, (10 ** (num_digits - 4)) - 1) 

# we assume only a single level of partition by date, which holds true for all of our experiments
# number of partitions is equal to the number of days in 6 years, which is what TPC-DS uses
def genData(data_config, data_set, table):
    SCHEMA = table["schema"]
    PARTITION_SCHEMA = table["partitionSchema"]
    start_date = date(1998, 1, 1)
    end_date = date(2003, 12, 31)
    date_list = [(start_date + timedelta(days=x)).strftime('%Y-%m-%d') for x in range((end_date - start_date).days + 1)]
    if (len(PARTITION_SCHEMA) != 0):
        files_per_partition = NUM_FILES // len(date_list)
        remainder = NUM_FILES % len(date_list)
        for i in range(len(date_list)):
            if remainder > 0:
                cur_files_per_partition = files_per_partition + 1
            else:
                cur_files_per_partition = files_per_partition
            for j in range(cur_files_per_partition):
                prefix = [ i ]
                genFile(data_set, SCHEMA, prefix, date_list)
            remainder -= 1
    else:
        for i in range(NUM_FILES):
            genFile(data_set, SCHEMA, [], date_list)
    return

# for now there 2 records are added per file to reduce relative amount of data.
# 2 records are enough to generate min and max values for each attribute
def genFile(data_set, schema, prefix, date_list):
    fst_record = prefix + [ ]
    snd_record = prefix + [ ]
    for attr in schema:
        if attr["type"] == "INT":
            fst_record.append(genInt(attr["cardinality"]))
            snd_record.append(genInt(attr["cardinality"]))
        # randomly generate string of given size         
        if attr["type"] == "VARCHAR":
            fst_record.append(genString(attr["cardinality"]))
            snd_record.append(genString(attr["cardinality"]))
        if attr["type"] == "DATE":
            fst_record.append(date_list[random.randint(0,len(date_list) - 1)])
            snd_record.append(date_list[random.randint(0,len(date_list) - 1)])
        if attr["type"] == "DECIMAL":
            fst_record.append(genDecimal(attr["cardinality"]))
            snd_record.append(genDecimal(attr["cardinality"]))
    data_set.append(fst_record)    
    data_set.append(snd_record)
    return

def main():
    # spark = SparkSession.builder \
    # .appName("Experiment1") \
    # .getOrCreate()
    with open("scanrangeconfig.json", "r") as template_file:
        data_config_json = template_file.read()
    data_config = json.loads(data_config_json)
    DB_NAME = data_config["iceberg"]
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
    spark.sql(f"USE {DB_NAME}")
    for table in data_config["tables"]:
        TABLE_NAME = table["name"]
        PARTITION_SCHEMA = table["partitionSchema"]
        SCHEMA = table["schema"]
        SCHEMA_NAMES = [ attr["name"] for attr in PARTITION_SCHEMA ] + [ attr["name"] for attr in SCHEMA ]
        INPUT_SCHEMA_NAMES = ["to_date(" + attr["name"] + ", 'yyyy-MM-dd')" if attr["type"] == "DATE" 
                              else attr["name"] for attr in PARTITION_SCHEMA ] + [ "to_date(" + attr["name"] + ", 'yyyy-MM-dd')" if attr["type"] == "DATE" 
                              else attr["name"] for attr in SCHEMA ]
        CREATE_TABLE_FIELDS = []
        for attr in PARTITION_SCHEMA:
            if attr["type"] == "VARCHAR":
                if not isinstance(attr["cardinality"], str):
                    CREATE_TABLE_FIELDS.append(attr["name"] + " VARCHAR(" + str(attr["cardinality"]) + ")")
                else:
                    CREATE_TABLE_FIELDS.append(attr["name"] + " VARCHAR(16)")
            elif attr["type"] == "DECIMAL":
                CREATE_TABLE_FIELDS.append(attr["name"] + " DECIMAL(" + str(attr["cardinality"] - 2) + ",2)")
            else:
                CREATE_TABLE_FIELDS.append(attr["name"] + " " + attr["type"])
        for attr in SCHEMA:
            if attr["type"] == "VARCHAR":
                if not isinstance(attr["cardinality"], str):
                    CREATE_TABLE_FIELDS.append(attr["name"] + " VARCHAR(" + str(attr["cardinality"]) + ")")
                else:
                    CREATE_TABLE_FIELDS.append(attr["name"] + " VARCHAR(16)")
            elif attr["type"] == "DECIMAL":
                CREATE_TABLE_FIELDS.append(attr["name"] + " DECIMAL(" + str(attr["cardinality"] - 2) + ",2)")
            else:
                CREATE_TABLE_FIELDS.append(attr["name"] + " " + attr["type"])    
        # drop the table if it already exists
        spark.sql(f"DROP TABLE IF EXISTS {DB_NAME}.{TABLE_NAME}")
        # create the table, partitioned as defined by the 
        if len(PARTITION_SCHEMA) > 0:
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {DB_NAME}.{TABLE_NAME} (
                    {','.join(CREATE_TABLE_FIELDS)}
                )
                USING iceberg
                PARTITIONED BY ({','.join([partition["name"] for partition in PARTITION_SCHEMA])})
            """)
        else:
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {DB_NAME}.{TABLE_NAME} (
                    {','.join(CREATE_TABLE_FIELDS)}
                USING iceberg
                )
            """)
        spark.sql(f"""
            CALL hive_prod.system.add_files(
                table => '{DB_NAME}.{TABLE_NAME}',
                source_table => '`parquet`.`hdfs:///spark_warehouse/{data_config["hms"]}.db/{TABLE_NAME}`'
            )
        """)
    return 0

main()