package zio.spark.rdd

import zio.spark.Fixture.read
import zio.spark.sql._
import zio.test._
import zio.test.Assertion._

object RDDTest {
  def rddActionsSpec: Spec[SparkSession, TestFailure[Any], TestSuccess] =
    suite("RDD Actions")(
      test("RDD should implement count correctly") {
        val job = read.map(_.rdd).flatMap(_.count)

        job.map(assert(_)(equalTo(4L)))
      },
      test("RDD should implement collect correctly") {
        val job = read.map(_.rdd).flatMap(_.collect)

        job.map(assert(_)(hasSize(equalTo(4))))
      }
    )

  def rddTransformationsSpec: Spec[SparkSession, TestFailure[Any], TestSuccess] =
    suite("RDD Transformations")(
      test("RDD should implement count correctly") {
        val job = read.map(_.rdd).flatMap(_.count)

        job.map(assert(_)(equalTo(67L)))
      }
    )
}
