package com.atguigu.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory_Par {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    //TODO 创建RDD
    //RDD的并行度和分区，makeRDD方法可以传递第二个参数，表示分区的数量
    val rdd = sc.makeRDD(List(1,2,3,4), 2)
    //奖处理的数据保存为分区文件
    rdd.saveAsTextFile("aout/output1-0")
    //TODO 关闭环境
    sc.stop()
  }
}
