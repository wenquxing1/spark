package com.atguigu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark08_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //RDD算子-sample
    val rdd = sc.makeRDD(List(1,2,3,4,5,6,7,8,9,10))

//    println(rdd.sample(
//      false,
//      0.4
////      1
//    ).collect().mkString(" "))

    println(rdd.sample(
      true,
      2
      //      1
    ).collect().mkString(" "))
    sc.stop()
  }
}
