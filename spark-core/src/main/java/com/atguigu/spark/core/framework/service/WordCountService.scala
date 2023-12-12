package com.atguigu.spark.core.framework.service

import com.atguigu.spark.core.framework.dao.WordCountDao
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
class WordCountService {
  private var wordCountDao = new WordCountDao()

  //数据分析
  def dataAnalyse() = {
    val lines = wordCountDao.readFile("datas/2.txt")
    val words: RDD[String] = lines.flatMap(_.split(" "))
    val wordToOne = words.map(word => (word, 1))
    val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
    val array: Array[(String, Int)] = wordToSum.collect()
    array

  }
}
