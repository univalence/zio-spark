package zio.spark.test

import zio._
import zio.spark.sql._
import zio.test._

abstract class SharedZIOSparkSpecDefault extends ZIOSpec[SparkSession] {
  def ss = defaultSparkSession

  @SuppressWarnings(Array("scalafix:DisableSyntax.valInAbstract"))
  override val bootstrap: TaskLayer[SparkSession] =
    ss.asLayer
      .tap(_.get.sparkContext.setLogLevel("ERROR"))
}
