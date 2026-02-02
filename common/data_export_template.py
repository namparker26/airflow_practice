from pyspark.sql import SparkSession
import os 
import sys

db_path = f"{os.getcwd()}"
# db_path = "/Users/namnguyen/Documents/Documents on MacPro/Projects/Airflows/database.sqlite"
spark = (
    SparkSession.builder
    .appName("SoccerSQLite")
    .config("spark.jars.packages", "org.xerial:sqlite-jdbc:3.48.0.0")
    .getOrCreate()
) 
jdbc_url = f"jdbc:sqlite:{db_path}"

class SqliteDataObject():
    def __init__(
            self,
            table_name
            ):
        self.table_name = table_name
        self.df = self.query_from_sqllite()

    def query_from_sqllite(self):
        df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", self.table_name)
            .option("driver", "org.sqlite.JDBC")
            .load()
        )
        return df
    def write_to_parquet(self):
        output_path = f'{db_path}/data/{self.table_name}'
        self.df.write.mode("overwrite").parquet(output_path)