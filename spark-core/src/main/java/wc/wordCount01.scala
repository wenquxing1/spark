package wc

import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.OL
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.util.parsing.json.JSON.headOptionTailToFunList

object wordCount01 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    wordcount9(sc)


    sc.stop()
  }
  def wordcount1(sc: SparkContext) = {
    val rdd = sc.makeRDD(List("hello scala", "hello spark"))
    val words = rdd.flatMap(_.split(" "))
    val group: RDD[(String, Iterable[String])] = words.groupBy(word => word)
    val wordCount: RDD[(String, Int)] = group.mapValues(iter => iter.size)
  }

  def wordcount2(sc: SparkContext) = {
    val rdd = sc.makeRDD(List("hello scala", "hello spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne = words.map((_,1))
    val group: RDD[(String, Iterable[Int])] = wordOne.groupByKey()
    val wordCount: RDD[(String, Int)] = group.mapValues(iter => iter.size)
  }

  def wordcount3(sc: SparkContext) = {
    val rdd = sc.makeRDD(List("hello scala", "hello spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne = words.map((_, 1))
    val wordCount: RDD[(String, Int)] = wordOne.reduceByKey(_ + _)
  }

  def wordcount4(sc: SparkContext) = {
    val rdd = sc.makeRDD(List("hello scala", "hello spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne = words.map((_, 1))
    val wordCount: RDD[(String, Int)] = wordOne.aggregateByKey(0)(_ + _, _ + _)
  }

  def wordcount5(sc: SparkContext) = {
    val rdd = sc.makeRDD(List("hello scala", "hello spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne = words.map((_, 1))
    val wordCount: RDD[(String, Int)] = wordOne.foldByKey(0)( _ + _)
  }

  def wordcount6(sc: SparkContext) = {
    val rdd = sc.makeRDD(List("hello scala", "hello spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne = words.map((_, 1))
    val wordCount: RDD[(String, Int)] = wordOne.combineByKey(
      v => v,
      (x: Int, y) => x + y,
      (x: Int, y: Int) => x + y
    )
  }

  def wordcount7(sc: SparkContext) = {
    val rdd = sc.makeRDD(List("hello scala", "hello spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne = words.map((_, 1))
    val wordCount: collection.Map[String, Long] = wordOne.countByKey()
  }

  def wordcount8(sc: SparkContext) = {
    val rdd = sc.makeRDD(List("hello scala", "hello spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordCount: collection.Map[String, Long] = words.countByValue()
  }

  def wordcount9(sc: SparkContext) = {
    val rdd = sc.makeRDD(List("hello scala", "hello spark"))
    val words = rdd.flatMap(_.split(" "))
    val mapWord = words.map(
      word => {
        mutable.Map[String, Long]((word, 1))
      }
    )
    val wordCount = mapWord.reduce(
      (map1, map2) => {
        map2.foreach {
          case (word, count) => {
            val newCount = map1.getOrElse(word, 0L) + count
            map1.update(word, newCount)
          }
        }
        map1
      }
    )
    print(wordCount)

  }
}
