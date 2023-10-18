package com.atguigu.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark017_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //RDD算子-(key,value)类型 groupbykey
    val rdd = sc.makeRDD(List(("a",1), ("a",2), ("a",3), ("a",4),("b",12),("b",10)), 2)
    //如果聚合计算时，分区内和分区间的计算规则相同，可用foldByKey此简化方法
    rdd.foldByKey(0)(_+_).collect().foreach(println)

    sc.stop()
  }
}
