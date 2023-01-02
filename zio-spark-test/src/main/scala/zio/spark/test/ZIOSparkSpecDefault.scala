package zio.spark.test

import zio._
import zio.spark.sql._
import zio.test._

abstract class ZIOSparkSpecDefault extends ZIOSpecDefault {
  def ss = defaultSparkSession

  def sparkSpec: Spec[SparkSession with TestEnvironment with Scope, Any]

  override def spec: Spec[TestEnvironment with Scope, Any] =
    sparkSpec.provideSomeLayer[TestEnvironment with Scope](
      ss.asLayer
        .tap(_.get.sparkContext.setLogLevel("ERROR"))
    )
}
