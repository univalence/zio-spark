package zio.spark.experimental

import scala3encoders.given // scalafix:ok

import zio.{Task, ZIO}
import zio.spark.experimental.MapWithEffect.RDDOps
import zio.spark.rdd.RDD
import zio.spark.sql._
import zio.spark.sql.implicits._
import zio.test._

object MapWithEffectSpec {
  def spec = suite("smoke")()
}
