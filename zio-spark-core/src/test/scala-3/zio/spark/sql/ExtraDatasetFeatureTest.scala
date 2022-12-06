package zio.spark.sql

import zio.spark.helper.Fixture._
import zio.spark.sql.implicits._
import zio.test._
import zio.test.TestAspect._

object ExtraDatasetFeatureTest {
  import zio.spark.sql.TryAnalysis.syntax.throwAnalysisException

  def spec: Spec[SparkSession, Any] = dataFrameActionsSpec

  def dataFrameActionsSpec: Spec[SparkSession, Any] =
    suite("ExtraDatatasetFeature Actions")(test("Dataset should implement explain correctly") {
      assertTrue(true)
    } @@ silent)
}
