package zio.spark

import org.apache.log4j.{Level, Logger}

import zio._
import zio.spark.parameter.localAllNodes
import zio.spark.rdd.{PairRDDFunctionsTest, RDDTest}
import zio.spark.sql.{
  DataFrameReaderTest,
  DataFrameWriterTest,
  DatasetTest,
  ExtraDatasetFeatureTest,
  RelationalGroupedDatasetTest,
  SparkSession
}
import zio.test._

/** Runs all spark specific tests in the same spark session. */
object ZioSparkTestSpec extends DefaultRunnableSpec {
  Logger.getLogger("org").setLevel(Level.OFF)

  val session: ZLayer[Any, Nothing, SparkSession] =
    SparkSession.builder
      .master(localAllNodes)
      .appName("zio-spark")
      .getOrCreate
      .toLayer
      .orDie

  type SparkTestEnvironment = TestEnvironment with SparkSession
  type SparkTestSpec        = Spec[SparkTestEnvironment, TestFailure[Any], TestSuccess]

  def spec: Spec[TestEnvironment, TestFailure[Any], TestSuccess] = {
    val specs =
      Seq(
        DatasetTest.datasetActionsSpec,
        DatasetTest.datasetTransformationsSpec,
        DatasetTest.sqlSpec,
        DatasetTest.persistencySpec,
        DatasetTest.errorSpec,
        DatasetTest.fromSparkSpec,
        DataFrameReaderTest.dataFrameReaderReadingSpec,
        DataFrameWriterTest.dataFrameWriterSavingSpec,
        DataFrameWriterTest.dataFrameWriterOptionDefinitionsSpec,
        ExtraDatasetFeatureTest.spec,
        RDDTest.rddActionsSpec,
        RDDTest.rddTransformationsSpec,
        PairRDDFunctionsTest.spec,
        RelationalGroupedDatasetTest.relationalGroupedDatasetAggregationSpec
      )

    suite("Spark tests")(specs: _*).provideCustomLayerShared(session)
  }
}
