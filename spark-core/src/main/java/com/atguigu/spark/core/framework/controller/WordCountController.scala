package com.atguigu.spark.core.framework.controller

import com.atguigu.spark.core.framework.commen.TraitController
import com.atguigu.spark.core.framework.service.WordCountService

class WordCountController extends TraitController{
  private var wordCountService = new WordCountService()
  //调度
  def dispatch(): Unit = {
    val array = wordCountService.dataAnalyse()
    array.foreach(println)
  }
}
