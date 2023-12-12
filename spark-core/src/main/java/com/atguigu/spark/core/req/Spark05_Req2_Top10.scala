package com.atguigu.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_Req2_Top10 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("hotTop")
    val sc = new SparkContext(sparkConf)
    //读取原始日志数据
    val actionRDD = sc.textFile("datas/user_visit_action.csv")
    actionRDD.cache()
    val top10Ids: Array[String] = top10Category(actionRDD)
    val filterActionRDD = actionRDD.filter(
      action => {
        val datas = action.split(",")
        if(datas(6) != "-1"){
          top10Ids.contains(datas(6))
        }else{
          false
        }
      }
    )
    val reduceRDD: RDD[((String, String), Int)] = filterActionRDD.map(
      action => {
        val datas = action.split(",")
        ((datas(6), datas(2)), 1)
      }
    ).reduceByKey(_ + _)

    val mapRDD = reduceRDD.map {
      case ((cid, sid), sum) => {
        (cid, (sid, sum))
      }
    }

    val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD.groupByKey()

    val resultRDD = groupRDD.mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
      }
    )
    resultRDD.collect().foreach(println)
    sc.stop()
  }

  def top10Category(actionRDD:RDD[String]) = {
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

    analysisRDD.sortBy(_._2, false).take(10).map(_._1)
  }
}
