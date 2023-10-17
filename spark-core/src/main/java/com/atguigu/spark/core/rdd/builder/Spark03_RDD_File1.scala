package com.atguigu.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_File1 {
  def main(args: Array[String]): Unit = {
    //准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    //创建RDD
    //textFile 以行为单位来读取数据
    //wholeTextFiles 以文件为单位读取数据，结果为元组，第一个元素为文件路径第二个元素为文件内容
    val rdd: RDD[(String, String)] = sc.wholeTextFiles("datas")
    rdd.collect().foreach(println)
    //关闭环境
    sc.stop()
  }
}

