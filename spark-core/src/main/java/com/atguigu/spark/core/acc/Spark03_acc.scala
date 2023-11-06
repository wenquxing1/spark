package com.atguigu.spark.core.acc

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_acc {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("acc")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(1,2,3,4))
    //获取系统累加器
    val sumAcc = sc.longAccumulator("sum")

    val mapRDD = rdd.map(
      num => {
        sumAcc.add(num)
        num
      }
    )

    mapRDD.collect()
    mapRDD.collect()
    println(sumAcc.value)

    sc.stop()
  }
}
