package com.atguigu.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark05_Bc {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("bc")
    val sc = new SparkContext(sparkConf)
    val rdd1 = sc.makeRDD(List(
      ("a",1),("b",2),("c",3)
    ))
//    val rdd2 = sc.makeRDD(List(
//      ("a", 4), ("b", 5), ("c",6)
//    ))
    val map = mutable.Map(("a",4), ("b", 5), ("c", 6))
    //join会导致数据量几何增长并影响shuffle的性能
//    val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
    rdd1.map {
      case (w, c) => {
        val i: Int = map.getOrElse(w, 0)
        (w, (w, i))
      }
    }.collect().foreach(println)
//    joinRDD.collect().foreach(println)
  }
}
