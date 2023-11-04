package com.atguigu.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

import java.io.Serializable

object Spark06_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1,2,3,4))

    // Driver内存集合循环遍历打印
    rdd.collect().foreach(println)
    println("----------------")
    //这样是分布式执行的  Executor端内存数据打印
    rdd.foreach(println)

    val user = new User
    rdd.foreach(
      num => {
        println("age= " + (user.age + num))
      }
    )
    sc.stop()
  }
//  class User extends Serializable{
  //样例类会在编译时自动混入序列化特质
  case class User() {
    val age: Int = 10
  }
}
