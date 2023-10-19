package com.atguigu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark023_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //RDD算子-(key,value)类型
    val rdd1 = sc.makeRDD(List(("a",1), ("b", 2), ("a", 3)))
    val rdd2 = sc.makeRDD(List(("a", 4), ("d", 5), ("c",6), ("c", 7)))
    val cogrouprdd: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)
    cogrouprdd.collect().foreach(println)
    println("++++++")


    sc.stop()
  }
}
