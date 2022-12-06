package zio.spark.rdd

import zio.spark.helper.Fixture.{read, Person}
import zio.spark.sql.{SIO, SparkSession}
import zio.test._
import zio.spark.sql.TryAnalysis.syntax.throwAnalysisException
import zio.spark.sql.implicits._

object RDDSpec {
  val readRDD: SIO[RDD[Person]] = read.map(_.as[Person].rdd)

  def rddActionsSpec: Spec[SparkSession, Any] =
    suite("RDD Actions")(
      test("RDD should implement count correctly") {
        for {
          df     <- readRDD
          output <- df.count
        } yield assertTrue(output == 4L)
      },
      test("RDD should implement collect correctly") {
        for {
          df     <- readRDD
          output <- df.collect
        } yield assertTrue(output == 4L)
      }
    )

  def rddTransformationsSpec: Spec[SparkSession, Any] =
    suite("RDD Transformations")(
      test("RDD should implement map correctly") {
        for {
          df <- readRDD
          transformedDf = df.map(_.age)
          output <- transformedDf.collect
        } yield assertTrue(output.headOption.get == 93)
      }
    )
}
