package io.univalence.sparkzio

import org.apache.spark.sql.{ DataFrame, Dataset, SparkSession }
import scalaz.zio.{ Task, TaskR, ZIO }

case class OptionSpark(
  key: String   = "",
  value: String = ""
)

trait SparkEnv {

  trait Read {
    def option(key: String, value: String): Read

    def parquet(path: String): Task[DataFrame]

    def textFile(path: String): Task[DataFrame]
  }

  def read: Read

  trait Write {
    def option(key: String, value: String): Write

    def textFile(path: String): Task[Unit]

    def parquet(path: String): Task[Unit]

    def cache: Task[Unit]
  }

  def write[T](ds: Dataset[T]): Write

  object implicits {

    implicit class DsOps[T](ds: Dataset[T]) {
      def zcache: Task[Dataset[T]] = Task.effect(ds.cache)

      def zwrite: Write = write(ds)
    }
  }

  trait Query {
    def sql(query: String): Task[DataFrame]
  }
  def query: Query
}

class SparkZIO(ss: SparkSession) extends SparkEnv {

  val readTrait: Read = new Read {
    override def option(key: String, value: String): Read =
      ???

    override def parquet(path: String): Task[DataFrame] =
      Task.effect(ss.read.parquet(path))

    override def textFile(path: String): Task[DataFrame] =
      Task.effect(ss.read.textFile(path).toDF())
  }
  override def read: Read = readTrait

  override def write[T](ds: Dataset[T]): Write = new Write {
    override def option(key: String, value: String): Write =
      ???

    override def textFile(path: String): Task[Unit] =
      Task.effect(ds.write.text(path))

    override def parquet(path: String): Task[Unit] =
      Task.effect(ds.write.parquet(path))

    override def cache: Task[Unit] =
      Task.effect(ds.cache())
  }

  val queryTrait: Query = new Query {
    override def sql(query: String): Task[DataFrame] =
      Task.effect(ss.sql(query))
  }

  override def query: Query = queryTrait
}

object SparkEnv {
  type TaskS[X] = TaskR[SparkEnv, X]

  def sql(query: String): TaskS[DataFrame] =
    ZIO.accessM(_.query.sql(query))
}
