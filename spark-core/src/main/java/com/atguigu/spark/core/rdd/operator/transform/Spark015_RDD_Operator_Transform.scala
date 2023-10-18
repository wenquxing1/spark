package com.atguigu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark015_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //RDD算子-(key,value)类型 reduce
    val rdd = sc.makeRDD(List(("a",1), ("a",2), ("a",3), ("b",4), ("c",10)))
    val newrdd: RDD[(String, Int)] = rdd.reduceByKey((x: Int, y: Int) => {
      x + y
    })
    newrdd.collect().foreach(println)

    sc.stop()
  }
}
