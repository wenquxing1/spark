package com.atguigu.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory {
  def main(args: Array[String]): Unit = {
    //准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    //创建环境
    val seq = Seq[Int](1,2,3,4)
    val rdd: RDD[Int] = sc.parallelize(seq)

    rdd.collect().foreach(println)
    //关闭环境
    sc.stop()
  }
}
