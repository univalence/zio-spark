package zio.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark

import zio.{UIO, ZIO, ZLayer}
import zio.spark.parameter.localAllNodes
import zio.spark.rdd.{PairRDDFunctionsTest, RDDTest}
import zio.spark.sql.{DatasetTest, ExtraDatasetFeatureTest, SparkSession}
import zio.test.{DefaultRunnableSpec, Spec, TestEnvironment, TestFailure, TestSuccess, ZSpec}
import zio.test.TestAspect.sequential

private object TestLocalSparkSession {
  lazy val session: spark.sql.SparkSession =
    org.apache.spark.sql.SparkSession.builder().master(localAllNodes.toString).appName("zio-spark-test").getOrCreate()
}

/** Runs all spark specific tests in the same spark session. */
object SparkSessionRunner extends DefaultRunnableSpec {
  Logger.getLogger("org").setLevel(Level.OFF)

  val session: ZLayer[Any, Nothing, SparkSession] = ZLayer(UIO(SparkSession(TestLocalSparkSession.session)))

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

    suite("Spark tests")(specs: _*).provideCustomLayerShared(session)
  }
}

/* object RunOneTest extends DefaultRunnableSpec { Logger.getLogger("org").setLevel(Level.OFF)
 *
 * override def spec: ZSpec[TestEnvironment, Any] =
 * suite("one")(DatasetTest.persistDataFrameTest).
 * provideCustomLayerShared(SparkSessionRunner.session) } */
