package com.atguigu.spark.core.rdd.persist

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Persist {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("persist")
    val sc = new SparkContext(sparkConf)
    //设置保存路径
    sc.setCheckpointDir("cp")
    val list = List("hello spark", "hello scala")
    val rdd = sc.makeRDD(list)
    val flatRDD = rdd.flatMap(_.split(" "))
    val mapRDD = flatRDD.map(word => {
      println("------")
      (word, 1)
    })
    //持久化操作   指定检查点保存路径，需要落盘
    mapRDD.checkpoint()

    val reduceRDD = mapRDD.reduceByKey(_ + _)
    reduceRDD.collect().foreach(println)
    println("**********")
    val groupRDD = mapRDD.groupByKey()
    groupRDD.collect().foreach(println)
  }
}
