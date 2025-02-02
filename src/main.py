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

    # Load CSV file
    csv_path = "/Users/jesperthoftillemannjaeger/PycharmProjects/delta_lake/download/co2_mm_gl.csv"
    #test = "/Users/jesperthoftillemannjaeger/PycharmProjects/delta_lake/README.md"
    #df = spark.read.format("csv").option("header", "true").option("inferSchema", "false").load(csv_path)
    df.show()
    df.head(2)
    # Define Delta table path
    delta_table_path = "/Users/jesperthoftillemannjaeger/PycharmProjects/delta_lake/download/delta_table"

    # Write data as Delta Lake format
    df.write.format("delta").mode("overwrite").save(delta_table_path)

    print(f"Delta table created at {delta_table_path}")

    #dt = DeltaTable("../download/co2_mm_gl.csv")
    dt = DeltaTable("/Users/jesperthoftillemannjaeger/PycharmProjects/delta_lake/download/co2_mm_gl.csv")

    print(2)