package com.atguigu.spark.core.rdd.io

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_IO_Load {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("load")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.textFile(".\\aout\\output2-0")
    println(rdd.collect().mkString("-"))

    val rdd1 = sc.objectFile[(String, Int)](".\\aout\\output2-1")
    println(rdd1.collect().mkString(","))

    val rdd2 = sc.sequenceFile[String, Int](".\\aout\\output2-2")
    println(rdd2.collect().mkString("*"))

    sc.stop()
  }
}
