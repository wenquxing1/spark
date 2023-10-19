package com.atguigu.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(1,2,3,4))
    //TODO 行动算子
//    val i: Int = rdd.reduce(_+_)
    val ints: Array[Int] = rdd.collect()
    println(ints.mkString("-"))
    val cnt: Long = rdd.count()
    println(cnt)
    val fst: Int = rdd.first()
    println(fst)
    val tak: Array[Int] = rdd.take(3)
    println(tak.mkString(","))

    val rdd1 = sc.makeRDD(List(4,2,1,3))
    val ints1: Array[Int] = rdd1.takeOrdered(3)
    println(ints1.mkString(","))
  }
}
