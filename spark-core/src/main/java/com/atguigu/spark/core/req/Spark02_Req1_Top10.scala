package com.atguigu.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Req1_Top10 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("hotTop")
    val sc = new SparkContext(sparkConf)

    //读取原始日志数据
    val actionRDD = sc.textFile("datas/user_visit_action.csv")
    //统计品类的点击数量
    val clickActionRDD = actionRDD.filter(
      action => {
        val datas = action.split(",")
        datas(6) != "-1"
      }
    )
    val clickCountRDD: RDD[(String, Int)] = clickActionRDD.map(
      action => {
        val datas = action.split(",")
        (datas(6), 1)
      }
    ).reduceByKey(_+_)
    //统计品类的下单数量
    val orderActionRDD = actionRDD.filter(
      action => {
        val datas = action.split(",")
        datas != ""
      }
    )
    val orderCountRDD = orderActionRDD.flatMap(
      action => {
        val datas = action.split(",")
        val cid = datas(8)
        val cids = cid.split("-")
        cids.map(id => (id, 1))
      }
    ).reduceByKey(_+_)
    //统计品类的支付数量
    val payActionRDD = actionRDD.filter(
      action => {
        val datas = action.split(",")
        datas != ""
      }
    )
    val payCountRDD = orderActionRDD.flatMap(
      action => {
        val datas = action.split(",")
        val cid = datas(10)
        val cids = cid.split("-")
        cids.map(id => (id, 1))
      }
    ).reduceByKey(_ + _)
    //将品类进行排序，并取前10名，点击数量排序，下单数量排序，支付数量排序
    //（品类ID，（点击数量，下单数量，支付数量））
    val rdd1 = clickCountRDD.map{
      case (cid, cnt) => {
        (cid, (cnt, 0, 0))
      }
    }
    val rdd2 = orderCountRDD.map{
      case (cid, cnt) => {
        (cid, (0, cnt, 0))
      }
    }
    val rdd3 = payCountRDD.map{
      case (cid, cnt) => {
        (cid, (0, 0, cnt))
      }
    }
    //将三个数据源合并在一起，统一进行聚合计算
    val sourceRDD: RDD[(String, (Int, Int, Int))] = rdd1.union(rdd2).union(rdd3)
    val analysisRDD = sourceRDD.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )

    val resultRDD = analysisRDD.sortBy(_._2, false).take(10)
    resultRDD.foreach(println)
    sc.stop()
  }
}
