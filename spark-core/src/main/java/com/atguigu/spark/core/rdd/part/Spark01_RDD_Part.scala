package com.atguigu.spark.core.rdd.part

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Spark01_RDD_Part {
  def main(args: Array[String]): Unit = {
    val saprkConf = new SparkConf().setMaster("local[*]").setAppName("part")
    val sc = new SparkContext(saprkConf)

    val rdd = sc.makeRDD(List(
      ("nba", "......"),
      ("cba", "......"),
      ("wnba","......"),
      ("nba", "......")
    ),3)

    val newrdd: RDD[(String, String)] = rdd.partitionBy(new MyPartitioner)
    newrdd.saveAsTextFile("F:\\third\\spark01\\spark-learn\\aout\\output1-10")

    sc.stop()
  }

  class MyPartitioner extends Partitioner{
    override def numPartitions: Int = 3

    override def getPartition(key: Any): Int = {
      key match {
        case "nba" => 0
        case "wnba" => 1
        case _ => 2
      }
    }
  }
}
