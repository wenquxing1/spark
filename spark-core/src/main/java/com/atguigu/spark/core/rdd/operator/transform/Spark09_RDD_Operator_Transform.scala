package com.atguigu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark09_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //RDD算子-sample
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 1, 2, 3))
    val rdd1: RDD[Int] = rdd.distinct()
    rdd1.collect().foreach(println)
    sc.stop()
  }
}
