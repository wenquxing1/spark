package com.atguigu.spark.core.rdd.serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.Predef.genericArrayOps

object Spark01_RDD_Serial {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.makeRDD(Array("hello world","hello spark","hive","hadoop"))

    val search = new Search("h")
    search.getMatch1(rdd).collect().foreach(println)
    sc.stop()
  }

  class Search(query: String) extends Serializable {
    def isMatch(s: String): Boolean = {
      s.contains(this.query)
    }
    def getMatch1(rdd: RDD[String]): RDD[String] = {
      rdd.filter(isMatch)
    }
    def getMatch2(rdd: RDD[String]): RDD[String] = {
      rdd.filter(x => x.contains(query))
    }
  }
}
