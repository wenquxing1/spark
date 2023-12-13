package com.atguigu.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Spark01_SparkSQL_Basic {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("saprkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

//    val df: DataFrame = spark.read.json("datas/user.json")
//    df.show
    //dataframe => sql
//    df.createOrReplaceTempView("user")
//    spark.sql("select username from user").show
//    spark.sql("select avg(age) as avg_age from user").show
    //dataframe => dsl
    import spark.implicits._
//    df.select("age")
//    df.select('age + 1).show
//    df.select()
    //dataset
//    val seq = Seq(1,2,3,4)
//    val ds: Dataset[Int] = seq.toDS()
//    ds.show
    //rdd <-> dataframe
    val rdd = spark.sparkContext.makeRDD(List((1,"wen",20),(2,"qu",30)))
    val df: DataFrame = rdd.toDF("id","name","age")
    val rdd1: RDD[Row] = df.rdd
    //dataframe <-> dataset
    val ds: Dataset[User] = df.as[User]
    val df1: DataFrame = ds.toDF()
    //rdd <-> dataset
    val ds1: Dataset[User] = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }.toDS()
    val userRDD: RDD[User] = ds.rdd

    spark.close()
  }
  case class User(id: Int, name: String, age: Int)
}
