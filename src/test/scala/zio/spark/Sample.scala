package zio.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import zio.spark.ProtoMapWithEffetTest.tap
import zio.spark.SparkEnvImplicitClassTest.pathToto
import zio.test.{ assert, Assertion, TestResult }
import zio.{ Task, ZIO }

import scala.util.Either

object Sample {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder().master("local").appName("xxx").getOrCreate()

    import ss.implicits._
    println(ss.read.textFile("src/test/resources/toto/").as[String].take(1)(0))

  }
}

object Sample2 {

  def main(args: Array[String]): Unit = {

    val prg = ss
      .flatMap(_.ss)
      .map(ss => {
        val someThing: RDD[Task[Int]] = ss.sparkContext.parallelize(1 to 100).map(x => Task(x))

        val executed: RDD[Either[Throwable, Int]] = tap(someThing)(new Exception("rejected"))

        assert(executed.count())(Assertion.equalTo(100L))
      })

    println(zio.Runtime.default.unsafeRun(prg.flatMap(_.run)).isSuccess)
  }

}
