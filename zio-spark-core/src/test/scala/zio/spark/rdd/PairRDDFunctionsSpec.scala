package zio.spark.rdd

import scala3encoders.given
import zio.spark.helper.Fixture.readLorem
import zio.spark.sql.{Dataset, SparkSession}
import zio.spark.sql.implicits._
import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect._

object PairRDDFunctionsSpec {
  def spec: Spec[SparkSession, Any] =
    suite("PairRDDFunctionsTest Transformations")(
      test("PairRDDFunctionsTest should add reduce by key correctly") {
        val transformation: Dataset[String] => RDD[(String, Int)] =
          _.flatMap(_.split(" ")).map((_, 1)).rdd.reduceByKey(_ + _)

        val job = readLorem.map(transformation).flatMap(_.count)

        job.map(assert(_)(equalTo(67L)))
      }
    ) @@ scala211(ignore)
}
