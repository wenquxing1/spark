package com.atguigu.spark.core.framework.application

import com.atguigu.spark.core.framework.commen.TraitApplication
import com.atguigu.spark.core.framework.controller.WordCountController
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCountApplication extends App with TraitApplication{
  start(){
    var controller = new WordCountController()
    controller.dispatch()
  }
}
