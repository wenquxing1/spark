package com.atguigu.spark.core.rdd.io

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_IO_Save {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("io")
    val sc = new SparkContext(sparkConf)
    val rdd= sc.makeRDD(List(
      ("a",1),
      ("b",2),
      ("c",3)
    ))
    rdd.saveAsTextFile(".\\aout\\output2-0")
    rdd.saveAsObjectFile(".\\aout\\output2-1")
    rdd.saveAsSequenceFile(".\\aout\\output2-2")

    sc.stop()
  }
}
