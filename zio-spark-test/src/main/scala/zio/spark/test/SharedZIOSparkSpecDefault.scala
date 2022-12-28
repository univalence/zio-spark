package zio.spark.test

import org.apache.log4j._

import zio._
import zio.spark.sql._
import zio.test._

abstract class SharedZIOSparkSpecDefault extends ZIOSpec[SparkSession] {
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
  Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)

  def ss = defaultSparkSession

  @SuppressWarnings(Array("scalafix:DisableSyntax.valInAbstract"))
  override val bootstrap: TaskLayer[SparkSession] = ss.asLayer
}
