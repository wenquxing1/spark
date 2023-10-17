package com.atguigu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Transform2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //RDD算子-flatMap
    val rdd = sc.makeRDD(List(List(1,2),3,List(4,5)))
    val flatRDD = rdd.flatMap(
      datas =>{
        datas match{
          case list:List[_] => list
          case data => List(data)
        }
      }
    )
    flatRDD.collect().foreach(println)
    sc.stop()
  }
}
