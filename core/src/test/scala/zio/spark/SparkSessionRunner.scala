package zio.spark

import org.apache.log4j.{Level, Logger}

import zio.ZLayer
import zio.spark.parameter.localAllNodes
import zio.spark.rdd.{PairRDDFunctionsTest, RDDTest}
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

  def spec: Spec[TestEnvironment, TestFailure[Any], TestSuccess] = {
    val specs =
      Seq(
        DatasetTest.datasetActionsSpec,
        DatasetTest.datasetTransformationsSpec,
        DatasetTest.sqlSpec,
        DatasetTest.persistencySpec,
        DatasetTest.fromSparkSpec,
        ExtraDatasetFeatureTest.spec,
        RDDTest.rddActionsSpec,
        PairRDDFunctionsTest.spec
      )

    suite("Spark tests")(specs: _*).provideShared(session)
  }
}
