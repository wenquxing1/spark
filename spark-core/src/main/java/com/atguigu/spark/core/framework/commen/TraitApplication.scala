package com.atguigu.spark.core.framework.commen

import com.atguigu.spark.core.framework.controller.WordCountController
import com.atguigu.spark.core.framework.util.EnvUtil
import org.apache.spark.{SparkConf, SparkContext}

trait TraitApplication {
  def start(master: String = "local[*]", app:String = "Application")(op : => Unit): Unit = {
    val sparkConf = new SparkConf().setMaster(master).setAppName(app)
    val sc = new SparkContext(sparkConf)
    EnvUtil.put(sc)
    try {
      op
    } catch {
      case ex => println(ex.getMessage)
    }
    sc.stop()
    EnvUtil.clear()
  }
}
