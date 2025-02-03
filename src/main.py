from dataclasses import dataclass

from deltalake import DeltaTable
from pyspark.sql import DataFrame
import os
from src.ai_utility.src.download.FileDownLoader import FileDownloader
from src.ai_utility.src.data_from_modules.y_finance import DataFromYFinance

from pathlib import Path
from src.spark.spark_handler import SparkHandler



if __name__ == '__main__':
    #http: // localhost: 4040 / jobs /

    #url = "https://gml.noaa.gov/webdata/ccgg/trends/co2/co2_mm_gl.csv"
    #url = "https://gml.noaa.gov/webdata/ccgg/trends/co2/co2_daily_mlo.csv"

    #filedownloader = FileDownloader(url)
    #store_path = filedownloader.store_path

    yfinance = DataFromYFinance("AAPL")
    store_path = yfinance.store_file

    delta_table_path = Path("/Users/jesperthoftillemannjaeger/PycharmProjects/delta_lake/delta_table") / store_path.stem

    sparkHandler = SparkHandler("MyApp", store_path, "unknown")
    new_data_sdf = sparkHandler.run()
    if not os.path.exists(delta_table_path):
        print("Delta table does not exist. Creating new Delta table.")
        #os.makedirs(delta_table_path, exist_ok=True)
        new_data_sdf.write.format("delta").mode("overwrite").save(str(delta_table_path))
    else:
        existing_data_df = sparkHandler.load_delta_table(str(delta_table_path))

        unique_cols = yfinance.unique_columns

        new_records_df = new_data_sdf.join(existing_data_df, on=unique_cols, how="left_anti")

        if new_records_df.count() > 0:
            new_records_df.write.format("delta").mode("append").save(delta_table_path)
            print(f"Appended {new_records_df.count()} new records to the Delta table.")
        else:
            print("No new data to append.")

    print(f"Delta table created at {delta_table_path}")

    dt = DeltaTable(str(delta_table_path))

    history_df = dt.history()  # This will return a DataFrame with the history
    new_data_df = dt.to_pandas()
    print(new_data_sdf)
    print("Done")