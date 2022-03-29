package zio.spark

import org.apache.log4j.{Level, Logger}

import zio._
import zio.spark.experimental.PipelineSpec
import zio.spark.parameter.localAllNodes
import zio.spark.rdd.{PairRDDFunctionsSpec, RDDSpec}
import zio.spark.sql.{
  DataFrameReaderSpec,
  DataFrameWriterSpec,
  DatasetSpec,
  ExtraDatasetFeatureTest,
  RelationalGroupedDatasetSpec,
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
        DatasetSpec.datasetActionsSpec,
        DatasetSpec.datasetTransformationsSpec,
        DatasetSpec.sqlSpec,
        DatasetSpec.persistencySpec,
        DatasetSpec.errorSpec,
        DatasetSpec.fromSparkSpec,
        DataFrameReaderSpec.dataFrameReaderReadingSpec,
        DataFrameWriterSpec.dataFrameWriterBuilderSpec,
        DataFrameWriterSpec.dataFrameWriterSavingSpec,
        DataFrameWriterSpec.dataFrameWriterOptionDefinitionsSpec,
        ExtraDatasetFeatureTest.spec,
        RDDSpec.rddActionsSpec,
        RDDSpec.rddTransformationsSpec,
        PairRDDFunctionsSpec.spec,
        PipelineSpec.pipelineSpec,
        RelationalGroupedDatasetSpec.relationalGroupedDatasetAggregationSpec
      )

    suite("Spark tests")(specs: _*).provideCustomLayerShared(session)
  }
}
