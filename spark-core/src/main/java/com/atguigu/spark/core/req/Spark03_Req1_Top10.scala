package com.atguigu.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Req1_Top10 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("hotTop")
    val sc = new SparkContext(sparkConf)

    //读取原始日志数据
    val actionRDD = sc.textFile("datas/user_visit_action.csv")
    //统计品类的点击数量
    //将品类进行排序，并取前10名，点击数量排序，下单数量排序，支付数量排序
    //（品类ID，（点击数量，下单数量，支付数量））
    val flatRDD: RDD[(String, (Int, Int, Int))] = actionRDD.flatMap(
      action => {
        val datas = action.split(",")
        if (datas(6) != "-1") {
          List((datas(6), (1, 0, 0)))
        } else if (datas(8) != "") {
          val ids = datas(8).split("-")
          ids.map(id => (id, (0, 1, 0)))
        } else if (datas(10) != "") {
          val ids = datas(10).split("-")
          ids.map(id => (id, (0, 0, 1)))
        } else {
          Nil
        }
      }
    )
    //将三个数据源合并在一起，统一进行聚合计算

    val analysisRDD = flatRDD.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )

    val resultRDD = analysisRDD.sortBy(_._2, false).take(10)
    resultRDD.foreach(println)
    sc.stop()
  }
}
