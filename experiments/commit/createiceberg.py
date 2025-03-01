import json
import random
import pandas as pd
from datetime import date, timedelta

def main():
    # spark = SparkSession.builder \
    # .appName("Experiment1") \
    # .getOrCreate()
    with open("commitconfig.json", "r") as template_file:
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
    return 0

main()