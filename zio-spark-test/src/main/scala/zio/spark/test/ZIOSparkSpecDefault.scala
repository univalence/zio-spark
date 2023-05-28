package zio.spark.test

import zio._
import zio.spark.sql._
import zio.test._

abstract class ZIOSparkSpecDefault extends ZIOSpec[TestEnvironment with SparkSession] {

  override def bootstrap: ZLayer[Any, Any, TestEnvironment with SparkSession] =
    testEnvironment ++ defaultSparkSession.asLayer.tap(_.get.sparkContext.setLogLevel("ERROR"))

}
