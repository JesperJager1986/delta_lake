import pyspark
from delta import configure_spark_with_delta_pip
import pandas as pd
from pyspark.sql import DataFrame
from deltalake import DeltaTable


class Sparkhandler:
    def __init__(self, name: str):
        self.name = name
        self.spark = self.build()


    def build(self):
        builder = pyspark.sql.SparkSession.builder.appName(self.name) \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        return configure_spark_with_delta_pip(builder).getOrCreate()

    def read_csv_to_dataframe(self, csv_path: str) -> DataFrame:
        """
        Reads a CSV file into a Spark DataFrame.
        :param csv_path: Path to the CSV file.
        :return: Spark DataFrame containing the CSV data.
        """
        return self.spark.read.option("comment", "#").csv(csv_path, header=True, inferSchema="true")

    @staticmethod
    def write_to_delta(df: DataFrame, delta_table_path: str) -> None:
        """
        Writes the DataFrame to a Delta table.
        :param df: DataFrame to be written.
        :param delta_table_path: Path to store the Delta table.
        """
        df.write.format("delta").mode("overwrite").save(delta_table_path)
        print(f"Delta table created at {delta_table_path}")

    @staticmethod
    def delta_to_pandas(self, delta_table_path: str) -> pd.DataFrame:
        """
        Converts a Delta table to a Pandas DataFrame.
        :param delta_table_path: Path to the Delta table.
        :return: Pandas DataFrame containing Delta table data.
        """
        dt = DeltaTable(delta_table_path)
        return dt.to_pandas()

    def into(self):
        pass