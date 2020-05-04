package zio.spark.v2

import org.apache.spark.sql.DataFrame
import zio.{ RIO, Task }

import scala.util.Try

trait ZSparkSession {

  def read: ZSparkSession.Read[Any] = ???
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

  trait Read[S] {
    def option(key: String, value: String): Read[S]
    def parquet(path: String): RIO[S, ZDataFrame]
    def textFile(path: String): RIO[S, ZDataFrame]
  }

  def read: Read[SparkEnv] = ???
}

object TestApp {

  val df: DataFrame = ???

  val value: SIO[ZDataFrame] = ZSparkSession.read.parquet("toto.parquet")

  def main(args: Array[String]): Unit = {}
}
