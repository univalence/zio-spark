package zio.spark

import org.apache.log4j.{Level, Logger}

import zio._
import zio.spark.experimental.{CancellableEffectSpec, PipelineSpec}
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
import zio.spark.experimental.MapWithEffectSpec
import zio.spark.sql.streaming.{DataStreamWriterSpec, StreamingSpec}
import zio.test._

/** Runs all spark specific tests in the same spark session. */
object ZioSparkTestSpec extends ZIOSpecDefault {
  Logger.getLogger("org").setLevel(Level.OFF)

  val session: ZLayer[Any, Nothing, SparkSession] =
    SparkSession.builder
      .master(localAllNodes)
      .appName("zio-spark")
      .asLayer
      .orDie

  type SparkTestEnvironment = TestEnvironment with SparkSession
  type SparkTestSpec        = Spec[SparkTestEnvironment, Any]

  def spec: Spec[TestEnvironment with Scope, Any] = {
    val specs: Seq[Spec[SparkTestEnvironment, Any]] =
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
        RelationalGroupedDatasetSpec.relationalGroupedDatasetAggregationSpec,
        CancellableEffectSpec.spec,
        StreamingSpec.streamingSpec,
        DataStreamWriterSpec.dataStreamReaderConfigurationsSpec,
        MapWithEffectSpec.spec
      )

    suite("Spark tests")(specs: _*).provideSomeLayerShared[TestEnvironment](session)
  }
}
