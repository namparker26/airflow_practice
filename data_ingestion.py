from pyspark.sql import SparkSession
from pyspark.sql.types import *
spark = (
    SparkSession.builder
    .appName("SoccerSourcing")
    .getOrCreate()
)

class SourcingColumn:
    def __init__(
            self, 
            field #spark schema.field
            ):
        self.column_name=field.name
        self.column_dtype=field.dataType
        self.column_nullable=field.nullable
    def convert_spark_to_databrick_dtype(self):
        if isinstance(self.column_dtype, StringType):
            return "STRING"
        if isinstance(self.column_dtype, IntegerType):
            return "INT"
        if isinstance(self.column_dtype, DecimalType):
            return f"DECIMAL({self.column_dtype.precision},{self.column_dtype.scale})"
            # return "FLOAT"
        # extend as needed...
        return self.column_dtype.simpleString().upper()
    def convert_nullable(self):
        if self.column_nullable == True:
            return 'NULL'
        if self.column_nullable == False:
            return 'NOT NULL'
    def generate_ddl_line (self):
        return f"{self.column_name} {self.convert_spark_to_databrick_dtype()}"

class SourcingTable:
    def __init__(
            self,
            volume_path, 
            folder_name, 
            target_schema_name, 
            target_table_name 
            ):
        self.volume_path = volume_path
        self.folder_name = folder_name
        self.target_schema_name = target_schema_name
        self.target_table_name = target_table_name
    def read_file(self):
        return spark.read.parquet(f'{self.volume_path}/{self.folder_name}')
    # def write_to_table(df, schema_name, table_name):
        # df.write.mode("overwrite").format("delta").saveAsTable(f'workspace.{schema_name}.{table_name}')
    def generate_ddl_query(self):
        df =self.read_file()
        table_schema = df.schema
        return f"CREATE TABLE IF NOT EXISTS workspace.{self.target_schema_name}.{self.target_table_name} (\n" + \
               ",\n".join([SourcingColumn(i).generate_ddl_line() for i in table_schema.fields]) + ")"