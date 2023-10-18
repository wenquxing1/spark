package com.atguigu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark017_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //RDD算子-(key,value)类型 groupbykey
    val rdd = sc.makeRDD(List(("a",1), ("a",2), ("a",3), ("a",4)), 2)
    //取出每个分区内相同key的最大值，然后分区间相加
    rdd.aggregateByKey(0)(
      (x, y) => math.max(x, y),
      (x, y) => x + y
    ).collect().foreach(println)

    sc.stop()
  }
}
