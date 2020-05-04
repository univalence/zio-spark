package zio.spark.v2

import org.apache.spark.sql.DataFrame
import zio.{ RIO, Task }

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

  trait RRead {
    def option(key: String, value: String): RRead
    def parquet(path: String): SIO[ZDataFrame]
    def textFile(path: String): SIO[ZDataFrame]
  }

  def read: RRead = ???
}

object TestApp {

  val df: DataFrame = ???

  val value: SIO[ZDataFrame] = ZSparkSession.read.parquet("toto.parquet")

  def main(args: Array[String]): Unit = {}
}
