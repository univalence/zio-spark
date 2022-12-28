package zio.spark.test

import org.apache.log4j._

import zio._
import zio.spark.sql._
import zio.test._

abstract class ZIOSparkSpecDefault extends ZIOSpecDefault {
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
  Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)

  def ss = defaultSparkSession

  def sparkSpec: Spec[SparkSession with TestEnvironment with Scope, Any]

  override def spec: Spec[TestEnvironment with Scope, Any] =
    sparkSpec.provideSomeLayer[TestEnvironment with Scope](ss.asLayer)
}
