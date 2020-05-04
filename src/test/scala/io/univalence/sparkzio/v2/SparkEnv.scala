package io.univalence.sparkzio.v2

import org.apache.spark.sql.DataFrame
import zio.Task

import scala.util.Try

trait ZSparkSession {

  trait Read {
    def option(key: String, value: String): Read
    def parquet(path: String): Task[ZDataFrame]
    def textFile(path: String): Task[ZDataFrame]
  }

  def read: Read = ???
}

trait ZDataFrame {

  trait Write {
    def parquet(path: String): Task[Unit]
  }

  def select(cols: String*): Try[ZDataFrame]

  def selectExpr(exprs: String*): Try[ZDataFrame]

  def write: Write
}

object ZSparkSession {

  trait Read {
    def option(key: String, value: String): Read
    def parquet(path: String): SIO[ZDataFrame]
    def textFile(path: String): SIO[ZDataFrame]
  }

  def read: Read = ???
}

object TestApp {

  val df: DataFrame = ???

  val value: SIO[ZDataFrame] = ZSparkSession.read.parquet("toto.parquet")
}
