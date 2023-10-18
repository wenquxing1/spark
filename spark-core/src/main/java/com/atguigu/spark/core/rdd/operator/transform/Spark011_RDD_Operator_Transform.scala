package com.atguigu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark011_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //RDD算子-sortBy
    val rdd: RDD[Int] = sc.makeRDD(List(6,2,4,5,1,3),2)
    val newrdd: RDD[Int] = rdd.sortBy(num => num)

    newrdd.saveAsTextFile("aout/output1-8")

    sc.stop()
  }
}
