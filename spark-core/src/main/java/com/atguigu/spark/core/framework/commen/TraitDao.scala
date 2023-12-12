package com.atguigu.spark.core.framework.commen

import com.atguigu.spark.core.framework.util.EnvUtil

trait TraitDao {
  def readFile(path: String)= {
    EnvUtil.take().textFile(path)
  }
}
