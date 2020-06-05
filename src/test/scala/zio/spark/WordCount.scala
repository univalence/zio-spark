/*
package zio.spark


import org.apache.spark.SparkContext
import org.apache.spark.sql.execution.QueryExecution
import zio.console.Console
import zio.spark.wrap.{Wrap, ZWrap}
import zio.{Task, ZIO, ZLayer, spark}

object WordCount extends zio.App {

  import zio.spark.implicits._

  /*
    val textFile = sc.textFile("hdfs://...")
    val counts = textFile.flatMap(line => line.split(" "))
                     .map(word => (word, 1))
                     .reduceByKey(_ + _)
    counts.saveAsTextFile("hdfs://...")
   */

  val prg: SIO[Unit] = {

    val textFile: ZDataset[String] = spark.read.textFile("hdfs://...")


    val counts = textFile.flatMap(line => line.split(" "))
                     .map(word => (word, 1))
                     .reduceByKey(_ + _))

    counts.saveAsTextFile("hdfs://...")

  }

  val ss: ZLayer[Any, Throwable, SparkEnv] =
    zio.spark.builder.master("local").appName("wordCount").getOrCreate

  override def run(args: List[String]): ZIO[zio.ZEnv, Nothing, Int] =
    prg.provideCustomLayer(ss).orDie

}
*/
