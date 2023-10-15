package wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object wordCount {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\winutils\\")
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val lines: RDD[String] = sc.textFile("datas")
    val words: RDD[String] = lines.flatMap(_.split(" "))

    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)

    val wordToCount = wordGroup.map{
      case (word, list) =>{(word, list.size)}
    }

    val array: Array[(String, Int)] = wordToCount.collect()
    array.foreach{ case (word, count) => println(s"$word: $count") }

    sc.stop();
  }
}
