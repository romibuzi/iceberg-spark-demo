package com.romibuzi

import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object IcebergWithSpark {
  def main(args: Array[String]): Unit = {
    System.setProperty("aws.accessKeyId", "minioadmin")
    System.setProperty("aws.secretAccessKey", "minioadmin")

    val conf = new SparkConf()
      .setAppName("Apache Iceberg with Spark")
      .setMaster("local[4]")
      .setAll(Seq(
        ("spark.driver.memory", "2g"),
        ("spark.executor.memory", "2g"),
        ("spark.sql.shuffle.partitions", "40"),

        // Add Iceberg SQL extensions like UPDATE or DELETE in Spark
        ("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"),

        // Register `my_iceberg_catalog`
        ("spark.sql.catalog.my_iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog"),

        // Configure SQL connection to track tables inside `my_iceberg_catalog`
        ("spark.sql.catalog.my_iceberg_catalog.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog"),
        ("spark.sql.catalog.my_iceberg_catalog.uri", "jdbc:postgresql://127.0.0.1:5432/iceberg_db"),
        ("spark.sql.catalog.my_iceberg_catalog.jdbc.user", "postgres"),
        ("spark.sql.catalog.my_iceberg_catalog.jdbc.password", "postgres"),

        // Configure Warehouse on MinIO
        ("spark.sql.catalog.my_iceberg_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"),
        ("spark.sql.catalog.my_iceberg_catalog.s3.endpoint", "http://127.0.0.1:9000"),
        ("spark.sql.catalog.my_iceberg_catalog.warehouse", "s3://warehouse"),
      ))

    val sc = new SparkContext(conf)
    sc.setLogLevel("INFO")

    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

    /* comment or uncomment methods you want below */

    createTable(spark)

    writeData(spark)

    // addColumn(spark)
    // addDataWithNewColumn(spark)
    // changePartitioning(spark)

    readData(spark)

    // dropTable(spark)

    sc.stop()
  }

  def createTable(spark: SparkSession) = {
    spark.sql("""
      CREATE TABLE IF NOT EXISTS my_iceberg_catalog.db.vaccinations (
        location string,
        date date,
        vaccine string,
        source_url string,
        total_vaccinations bigint,
        people_vaccinated bigint,
        people_fully_vaccinated bigint,
        total_boosters bigint
      ) USING iceberg PARTITIONED BY (location, date)"""
    )
  }

  def dropTable(spark: SparkSession) = {
    spark.sql("TRUNCATE TABLE my_iceberg_catalog.db.vaccinations;")
    spark.sql("DROP TABLE my_iceberg_catalog.db.vaccinations;")
  }

  def writeData(spark: SparkSession) = {
    val path: String = getClass.getResource("/covid19-vaccinations-country-data/Belgium.csv").getPath

    val vaccinations: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)

    vaccinations
      .withColumn("date", to_date(col("date")))
      .writeTo("my_iceberg_catalog.db.vaccinations")
      .append()
  }

  def readData(spark: SparkSession) = {
    val vaccinations = spark.table("my_iceberg_catalog.db.vaccinations")
    vaccinations.show(10)
  }

  def addColumn(spark: SparkSession) = {
    spark.sql("""
        ALTER TABLE my_iceberg_catalog.db.vaccinations
        ADD COLUMN people_fully_vaccinated_percentage double
      """
    )
  }

  def addDataWithNewColumn(spark: SparkSession) = {
    val path: String = getClass.getResource("/covid19-vaccinations-country-data/Spain.csv").getPath

    val spainVaccinations: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)

    spainVaccinations
      .withColumn("date", to_date(col("date")))
      .withColumn("people_fully_vaccinated_percentage", (col("people_fully_vaccinated") / col("people_vaccinated")) * 100)
      .writeTo("my_iceberg_catalog.db.vaccinations")
      .append()
  }

  def changePartitioning(spark: SparkSession) = {
    spark.sql("ALTER TABLE my_iceberg_catalog.db.vaccinations DROP PARTITION FIELD date")
    spark.sql("ALTER TABLE my_iceberg_catalog.db.vaccinations ADD PARTITION FIELD months(date)")

    // new data added will be partitioned by month
  }
}
