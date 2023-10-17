package com.atguigu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //RDD算子-flatMap
    val rdd: RDD[String] = sc.makeRDD(List("hello scala", "hello spark"))
    val flatRDD: RDD[String] = rdd.flatMap(s => {
      s.split(" ")
    })
    flatRDD.collect().foreach(println)

    sc.stop()
  }
}
