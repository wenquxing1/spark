package com.atguigu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark012_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //RDD算子-sortBy
    val rdd = sc.makeRDD(List(("2",12),("1",2),("10",5)), 2)
    val newrdd = rdd.sortBy(t=>t._1.toInt, false)



    newrdd.collect().foreach(println)

    sc.stop()
  }
}
