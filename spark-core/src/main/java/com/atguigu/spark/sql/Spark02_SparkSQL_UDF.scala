package com.atguigu.spark.sql

import org.apache.arrow.util.AutoCloseables.close
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object Spark02_SparkSQL_UDF {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("saprkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val df: DataFrame = spark.read.json("datas/user.json")
    df.createOrReplaceTempView("user")

    spark.udf.register("prefixName", (name: String) => {
      "Name:" + name
    })

    spark.sql("select age, prefixName(username) from user").show


    spark.close()
  }
}
