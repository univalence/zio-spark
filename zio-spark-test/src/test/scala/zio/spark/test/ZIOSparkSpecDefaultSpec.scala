package zio.spark.test

import scala3encoders.given // scalafix:ok

import zio.spark.sql.SparkSession
import zio.spark.sql.implicits._
import zio.test._

object ZIOSparkSpecDefaultSpec extends ZIOSparkSpecDefault {
  override def spec: Spec[SparkSession, Throwable] =
    suite("ZIOSparkSpecDefault can run spark job without providing layer")(
      test("It can run Dataset job") {
        for {
          df    <- Seq(1, 2, 3).toDS
          count <- df.count
        } yield assertTrue(count == 3L)
      },
      test("It can run RDD job") {
        for {
          rdd   <- Seq(1, 2, 3).toRDD
          count <- rdd.count
        } yield assertTrue(count == 3L)
      }
    )
}
