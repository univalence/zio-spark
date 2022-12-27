package zio.spark.rdd


import zio.spark.helper.Fixture.readLorem
import zio.spark.sql._
import zio.spark.sql.implicits._
import zio.spark.test._
import zio.test._
import zio.test.Assertion._

object PairRDDFunctionsSpec extends SharedZIOSparkSpecDefault {
  def spec: Spec[SparkSession, Throwable] =
    suite("PairRDDFunctionsTest transformations")(
      test("PairRDDFunctionsTest should add reduce by key correctly") {
        val transformation: Dataset[String] => RDD[(String, Int)] =
          _.flatMap(_.split(" ")).map((_, 1)).rdd.reduceByKey(_ + _)

        val job = readLorem.map(transformation).flatMap(_.count)

        job.map(assert(_)(equalTo(67L)))
      }
    )
}
