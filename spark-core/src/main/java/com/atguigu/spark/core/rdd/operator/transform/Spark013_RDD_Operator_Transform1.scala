package com.atguigu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark013_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //RDD算子-双value类型
    //两个数据源要求分区数量相等
    //两个数据源要求分区中的数据数量保持一致
    val rdd1 = sc.makeRDD(List(1,2,3,4,5,6))
    val rdd2 = sc.makeRDD(List(3,4,5,6))
    val rdd6: RDD[(Int, Int)] = rdd1.zip(rdd2)
    println(rdd6.collect().mkString(","))
    sc.stop()
  }
}
