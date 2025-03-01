import json
import random
import pandas as pd
from datetime import date, timedelta

DATA_FILE = "dstreeconfig.json"
SCALING_FACTOR = "100T"
# We assume 100 megabytes per file for now
BYTES_PER_FILE = 100000000

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
    num_rows = table["scaling"][SCALING_FACTOR]
    # we assume 4 bytes per field, which is somewhat adhoc
    bytes_per_row = 4 * (len(PARTITION_SCHEMA) + len(SCHEMA))
    total_bytes = bytes_per_row * num_rows
    # we assume size of each file is approx 100 megabytes
    rows_per_file = min(num_rows, BYTES_PER_FILE // bytes_per_row)
    file_size = bytes_per_row * rows_per_file
    num_files = total_bytes // file_size
    if (total_bytes % file_size) > 0:
        num_files += 1
    start_date = date(1998, 1, 1)
    end_date = date(2003, 12, 31)
    date_list = [(start_date + timedelta(days=x)).strftime('%Y-%m-%d') for x in range((end_date - start_date).days + 1)]
    if (len(PARTITION_SCHEMA) != 0):
        files_per_partition = num_files // len(date_list)
        remainder = num_files % len(date_list)
        cur_offset = 0
        for i in range(len(date_list)):
            if remainder > 0:
                cur_files_per_partition = files_per_partition + 1
            else:
                cur_files_per_partition = files_per_partition
            if cur_files_per_partition == 0:
                cur_files_per_partition = 1
            for j in range(cur_files_per_partition):
                prefix = [ i ]
                genFile(data_config["tables"], data_set, SCHEMA, cur_offset, rows_per_file, prefix, date_list)
                cur_offset += rows_per_file
            remainder -= 1
    else:
        cur_offset = 0
        for i in range(num_files):
            genFile(data_config["tables"], data_set, SCHEMA, cur_offset, rows_per_file, [], date_list)
            cur_offset += rows_per_file
    return

# for now there 2 records are added per file to reduce relative amount of data.
# 2 records are enough to generate min and max values for each attribute
def genFile(tables, data_set, schema, cur_offset, rows_per_file, prefix, date_list):
    fst_record = prefix + [ ]
    snd_record = prefix + [ ]
    for attr in schema:
        if attr["clustered"]:
            min_int = cur_offset
            max_int = cur_offset + rows_per_file - 1
            # add the surrogate keys
            if attr["type"] == "INT":
                fst_record.append(min_int)
                snd_record.append(max_int)
            # add the business_ids
            elif attr["type"] == "VARCHAR":
                fst_record.append(intToStr(16, min_int))
                snd_record.append(intToStr(16, max_int))
        else:
            if attr["type"] == "INT":
                if isinstance(attr["cardinality"], str) and attr["cardinality"] == "date_dim":
                    fst_record.append(random.randint(0,len(date_list) - 1))
                    snd_record.append(random.randint(0,len(date_list) - 1))
                # if surrogate key, the domain is the size of the dimension table
                elif isinstance(attr["cardinality"], str):
                    dim_table = getTable(tables, attr["cardinality"])
                    fst_record.append(genInt(dim_table["scaling"][SCALING_FACTOR]))
                    snd_record.append(genInt(dim_table["scaling"][SCALING_FACTOR]))
                # otherwise use the given cardinality    
                else:
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
    with open(DATA_FILE, "r") as template_file:
        data_config_json = template_file.read()
    data_config = json.loads(data_config_json)
    DB_NAMES = data_config["databaseNames"]
    for db_name in DB_NAMES:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
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
            spark.sql(f"DROP TABLE IF EXISTS {db_name}.{TABLE_NAME}")
            # create the table, partitioned as defined by the 
            if len(PARTITION_SCHEMA) > 0:
                spark.sql(f"""
                    CREATE TABLE IF NOT EXISTS {db_name}.{TABLE_NAME} (
                        {','.join(CREATE_TABLE_FIELDS)}
                    )
                    PARTITIONED BY ({','.join([partition["name"] for partition in PARTITION_SCHEMA])})
                    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
                    STORED AS TEXTFILE
                """)
            else:
                spark.sql(f"""
                    CREATE TABLE IF NOT EXISTS {db_name}.{TABLE_NAME} (
                        {','.join(CREATE_TABLE_FIELDS)}
                    )
                    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
                    STORED AS TEXTFILE
                """)
            data_set = []
            genData(data_config, data_set, table)
            data_df = spark.createDataFrame(data_set, SCHEMA_NAMES)
    #        if table["type"] == "dimension":
    #            data_df.repartitionByRange(len(data_set) // 2, SCHEMA[0]["name"])
            data_df.createOrReplaceTempView("inputData")
            data_df.printSchema()
            spark.sql(f"""
                INSERT INTO {db_name}.{TABLE_NAME} ({','.join(SCHEMA_NAMES)})
                SELECT {','.join(INPUT_SCHEMA_NAMES)} FROM inputData
            """)
    return 0

main()