package zio.spark.sql

import zio.test._

object ExtraDatatasetFeatureTest {
  def spec: Spec[TestEnvironment, TestFailure[Any], TestSuccess] = suite("ExtraDatatasetFeature Spec")()
}
