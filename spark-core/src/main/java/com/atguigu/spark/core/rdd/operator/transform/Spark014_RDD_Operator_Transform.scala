package com.atguigu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark014_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //RDD算子-双value类型
    val rdd = sc.makeRDD(List(1,2,3,4),2)
    val newrdd: RDD[(Int, Int)] = rdd.map((_, 1))
    //RDD =>PaiRDDFunction
    //隐式转换（二次编译）
    //partitionBy根据指定的分区规则对数据进行重分区
    newrdd.partitionBy(new HashPartitioner(2)).saveAsTextFile("aout/output1-9")

    sc.stop()
  }
}
