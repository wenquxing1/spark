package com.atguigu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark010_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //RDD算子-sample
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6),3)
//    val newrdd: RDD[Int] = rdd.coalesce(2)
    val newrdd: RDD[Int] = rdd.coalesce(2, true)

    newrdd.saveAsTextFile("aout/output1-6")

    sc.stop()
  }
}
