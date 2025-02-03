import pyspark
from delta import configure_spark_with_delta_pip
import pandas as pd
from pyspark.sql import DataFrame
from deltalake import DeltaTable


class SparkHandler:
    def __init__(self, name: str, store_path, header):
        self.name = name
        self.store_path: str = store_path
        self.header: str = header
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
        return self.spark.read.format("delta").option("comment", "#").csv(csv_path, header=True, inferSchema="true")

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
    def delta_to_pandas(delta_table_path: str) -> pd.DataFrame:
        """
        Converts a Delta table to a Pandas DataFrame.
        :param delta_table_path: Path to the Delta table.
        :return: Pandas DataFrame containing Delta table data.
        """
        dt = DeltaTable(delta_table_path)
        return dt.to_pandas()


    def find_header(self):
        sample_data = self.spark.read.option("comment", "#") \
            .option("inferSchema", "true") \
            .option("header", "false") \
            .csv(str(self.store_path)) \
            .limit(5)
        first_row = sample_data.collect()[0] if sample_data.count() > 0 is not None else None

        if first_row is not None:
            if all(isinstance(first_row, str) for first_row in first_row):
                header = "true"
            else:
                header = "false"
        else:
            raise Exception("There doesn't seem to be any data")

        return header

    def run(self):
        if self.header == "unknown":
            header = self.find_header()
        else:
            header = self.header

        df_data = self.spark.read.format("delta").option("comment", "#") \
                .option("inferSchema", "true") \
                .option("header", header) \
                .csv(str(self.store_path))
        return df_data


    def load_delta_table(self, delta_table_path: str) -> DataFrame:
        sdf = self.spark.read.format("delta").load(str(delta_table_path))
        return sdf


    def into(self):
        pass