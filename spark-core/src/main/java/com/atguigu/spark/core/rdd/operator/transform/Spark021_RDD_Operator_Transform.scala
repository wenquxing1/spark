package com.atguigu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark021_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //RDD算子-(key,value)类型
    val rdd1 = sc.makeRDD(List(("a",1), ("b", 2), ("a", 3)))
    val rdd2 = sc.makeRDD(List(("a", 4), ("d", 5), ("a", 6)))
    val newrdd: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
    newrdd.collect().foreach(println)


    sc.stop()
  }
}
