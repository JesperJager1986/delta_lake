from deltalake import DeltaTable
import pyspark
from delta import configure_spark_with_delta_pip
import pandas as pd
from pyspark.sql import DataFrame
import os
from src.ai_utility.src.download.FileDownLoader import FileDownloader
from pathlib import Path


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

    def write_to_delta(self, df: DataFrame, delta_table_path: str) -> None:
        """
        Writes the DataFrame to a Delta table.
        :param df: DataFrame to be written.
        :param delta_table_path: Path to store the Delta table.
        """
        df.write.format("delta").mode("overwrite").save(delta_table_path)
        print(f"Delta table created at {delta_table_path}")

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

if __name__ == '__main__':
    #http: // localhost: 4040 / jobs /

    #url = "https://gml.noaa.gov/webdata/ccgg/trends/co2/co2_mm_gl.csv"
    url = "https://gml.noaa.gov/webdata/ccgg/trends/co2/co2_daily_mlo.csv"

    filedownloader = FileDownloader(url)
    store_path = filedownloader.store_path
    delta_table_path = Path("/Users/jesperthoftillemannjaeger/PycharmProjects/delta_lake/delta_table") / store_path.stem

    sparkHandler = Sparkhandler("MyApp")
    spark = sparkHandler.build()

    sample_data = df_data = spark.read.option("comment", "#") \
    .option("inferSchema", "true") \
    .option("header", "true") \
    .csv(str(store_path)) \
    .limit(5)

    first_row = sample_data.collect()[0] if sample_data.count() > 0 is not None else None

    if first_row is not None:
        if all(isinstance(first_row, str) for first_row in first_row):
            header = "true"
        else:
            header = "false"

        df_data = spark.read.option("comment", "#") \
            .option("inferSchema", "true") \
            .option("header", header) \
            .csv(str(store_path)) \

    new_data_df: DataFrame = sparkHandler.read_csv_to_dataframe(csv_path=str(store_path) )

    if not os.path.exists(delta_table_path):
        print("Delta table does not exist. Creating new Delta table.")
        #os.makedirs(delta_table_path, exist_ok=True)
        new_data_df.write.format("delta").mode("overwrite").save(str(delta_table_path))
    else:
        existing_data_df = spark.read.format("delta").load(str(delta_table_path))

        unique_cols = ["year", "month"]

        new_records_df = new_data_df.join(existing_data_df, on=unique_cols, how="left_anti")

        if new_records_df.count() > 0:
            new_records_df.write.format("delta").mode("append").save(delta_table_path)
            print(f"Appended {new_records_df.count()} new records to the Delta table.")
        else:
            print("No new data to append.")

    print(f"Delta table created at {delta_table_path}")

    dt = DeltaTable(str(delta_table_path))

    history_df = dt.history()  # This will return a DataFrame with the history
    new_data_df = dt.to_pandas()
    print(new_data_df)
    print("Done")