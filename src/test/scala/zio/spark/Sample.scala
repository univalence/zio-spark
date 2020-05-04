package zio.spark

import org.apache.spark.sql.SparkSession
import zio.Task

object Sample {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder().master("local").appName("xxx").getOrCreate()

    import ss.implicits._
    println(ss.read.textFile("src/test/resources/toto/").as[String].take(1)(0))

  }
}

object Sample2 {

  def main(args: Array[String]): Unit =
    zio.Runtime.default.unsafeRun(ss)

}
