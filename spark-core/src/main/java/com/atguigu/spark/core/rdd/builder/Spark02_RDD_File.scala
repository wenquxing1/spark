package com.atguigu.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File {
  def main(args: Array[String]): Unit = {
    //准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    //创建RDD
    //从文件中创建RDD，将文件中的数据作为处理的数据源
    //val rdd: RDD[String] = sc.textFile("datas/1.txt")
    //path可以是具体的文件，也可以是目录
    //val rdd: RDD[String] = sc.textFile("datas")
    //path可以使用通配符
    val rdd: RDD[String] = sc.textFile("datas/1*.txt")
    rdd.collect().foreach(println)
    //关闭环境
    sc.stop()
  }
}

