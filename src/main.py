from deltalake import DeltaTable



if __name__ == '__main__':
    #http: // localhost: 4040 / jobs /
    import pyspark
    from delta import configure_spark_with_delta_pip
    builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    print(spark.version)

    csv_path = "/Users/jesperthoftillemannjaeger/PycharmProjects/delta_lake/download/co2_mm_gl.csv"
    df = spark.read.option("comment", "#").csv(csv_path, header=True, inferSchema="true")
    df.show()
    delta_table_path = "/Users/jesperthoftillemannjaeger/PycharmProjects/delta_lake/download/delta_table"

    df.write.format("delta").mode("overwrite").save(delta_table_path)

    print(f"Delta table created at {delta_table_path}")

    dt = DeltaTable(delta_table_path)
    df = dt.to_pandas()
    print(2)