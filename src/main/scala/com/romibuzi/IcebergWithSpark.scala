package com.romibuzi

import org.apache.spark.sql.SparkSession
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

    // spark now configured to use iceberg

    sc.stop()
  }
}
