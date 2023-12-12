package com.atguigu.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Req1_Top10 {
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
    val cogroupRDD:RDD[(String,(Iterable[Int],Iterable[Int],Iterable[Int]))]=clickCountRDD.cogroup(orderCountRDD,payCountRDD)
    val analysisRDD = cogroupRDD.mapValues{
      case (clickIter, orderIter, payIter) => {
        var clickCnt = 0
        val iter1 = clickIter.iterator
        if(iter1.hasNext){
          clickCnt = iter1.next()
        }
        var orderCnt = 0
        val iter2 = orderIter.iterator
        if(iter2.hasNext){
          orderCnt = iter2.next()
        }
        var payCnt = 0
        val iter3 = payIter.iterator
        if(iter3.hasNext){
          payCnt = iter3.next()
        }
        (clickCnt,orderCnt,payCnt)
      }
    }
    val resultRDD = analysisRDD.sortBy(_._2, false).take(10)
    resultRDD.foreach(println)
    sc.stop()
  }
}
