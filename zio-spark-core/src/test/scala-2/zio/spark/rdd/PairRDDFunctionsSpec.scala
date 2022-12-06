package zio.spark.rdd

import zio.spark.helper.Fixture.readLorem
import zio.spark.sql.{Dataset, SparkSession}
import zio.test.{assert, suite, test, Spec}
import zio.test.Assertion.equalTo
import zio.spark.sql.implicits._

object PairRDDFunctionsSpec {
  def spec: Spec[SparkSession, Any] =
    suite("PairRDDFunctionsTest Transformations")(
      test("PairRDDFunctionsTest should add reduce by key correctly") {
        val transformation: Dataset[String] => RDD[(String, Int)] =
          _.flatMap(_.split(" ")).map((_, 1)).rdd.reduceByKey(_ + _)

        val job = readLorem.map(transformation).flatMap(_.count)

        job.map(assert(_)(equalTo(67L)))
      }
    )
}
