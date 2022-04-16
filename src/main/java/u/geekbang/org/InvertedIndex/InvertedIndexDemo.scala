package u.geekbang.org.InvertedIndex

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author chen.xinliang
 * @create 2022-04-16 9:16
 */
object InvertedIndexDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val input = "datas"
    /**
     * 首先获取路径下的文件列表，unionRDD 按照wordcount来构建
     */

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val filelist = fs.listFiles(new Path(input), true)
    var unionrdd = sc.emptyRDD[(String,String)]
    while (filelist.hasNext){
      val abs_path = new Path(filelist.next().getPath.toString)
      val file_name = abs_path.getName
      val rdd1 = sc.textFile(abs_path.toString).flatMap(_.split(" ").map((file_name,_)))
      unionrdd = unionrdd.union(rdd1)
    }
    val rdd2 = unionrdd.map(word => {(word, 1)}).reduceByKey(_ + _)
    val frdd1 = rdd2.map(word =>{(word._1._2,String.format("(%s,%s)",word._1._1,word._2.toString))})
    val frdd2 = frdd1.reduceByKey(_ +"," + _)
    val frdd3 = frdd2.map(word =>String.format("\"%s\",{%s}",word._1,word._2))
    frdd3.foreach(println)
    sc.stop()
  }




}
