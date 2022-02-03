package zio.spark

import org.apache.log4j.{Level, Logger}

import zio.ZLayer
import zio.spark.parameter.localAllNodes
import zio.spark.sql.{DatasetTest, ExtraDatasetFeatureTest, SparkSession}
import zio.test.{DefaultRunnableSpec, Spec, TestEnvironment, TestFailure, TestSuccess}

/** Run all spark specific test in the same spark session. */
object SparkSessionRunner extends DefaultRunnableSpec {
  Logger.getLogger("org").setLevel(Level.OFF)

  val session: ZLayer[Any, Nothing, SparkSession] =
    SparkSession.builder
      .master(localAllNodes)
      .appName("zio-spark")
      .getOrCreateLayer
      .orDie

  def spec: Spec[TestEnvironment, TestFailure[Any], TestSuccess] =
    (DatasetTest.datasetActionsSpec + DatasetTest.datasetTransformationsSpec + DatasetTest.fromSparkSpec + ExtraDatasetFeatureTest.spec)
      .provideShared(session)
}
