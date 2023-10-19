package com.atguigu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark019_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //RDD算子-(key,value)类型 groupbykey
    val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4), ("b", 12), ("b", 10)), 2)
    //获取相同key的数据的平均值
    val newrdd: RDD[(String, (Int, Int))] = rdd.combineByKey(
      v => (v, 1),
      (t: (Int, Int), v) => {
        (t._1 + v, t._2 + 1)
      },
      (t1: (Int, Int), t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    )
    val resultrdd: RDD[(String, Int)] = newrdd.mapValues {
      case (num, cnt) => {
        num / cnt
      }
    }
    resultrdd.collect().foreach(println)


    sc.stop()
  }
}
