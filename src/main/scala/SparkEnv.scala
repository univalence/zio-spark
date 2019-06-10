package io.univalence.sparkzio

import org.apache.spark.sql.{ DataFrame, DataFrameReader, Dataset, SparkSession }
import scalaz.zio.{ Task, TaskR, ZIO }

final case class Write[T](ds: Dataset[T], options: Seq[(String, String)], format: Option[String]) {
  def option(key: String, value: String): Write[T] = this.copy(options = options :+ (key -> value))

  def text(path: String): Task[Unit] = Task(ds.write.options(options.toMap).text(path))

  def parquet(path: String): Task[Unit] = Task(ds.write.options(options.toMap).parquet(path))

  def cache: Task[Unit] = Task(ds.cache)

  def format(name: String): Write[T] = this.copy(format = Some(name))
}

object Write {
  def apply[T](ds: Dataset[T]): Write[T] = Write(ds, Nil, None)
}

trait SparkEnv {

  def read: SparkEnv.Read

  def write[T](ds: Dataset[T]): Write[T] = Write(ds)

  trait Query {
    def sql(query: String): Task[DataFrame]
  }

  def query: Query

  def ss: Task[SparkSession]
}

class SparkZIO(spark: SparkSession) extends SparkEnv {

  private case class ReadImpl(ops: Seq[(String, String)]) extends SparkEnv.Read {
    private def dataFrameReader: DataFrameReader = ops.foldLeft(spark.read)({ case (dfr, (k, v)) => dfr.option(k, v) })

    override def option(key: String, value: String): SparkEnv.Read = copy(ops :+ ((key, value)))

    override def parquet(path: String): Task[DataFrame] =
      Task(dataFrameReader.parquet(path))

    override def textFile(path: String): Task[DataFrame] =
      Task(dataFrameReader.textFile(path).toDF())
  }

  override def read: SparkEnv.Read = ReadImpl(Nil)

  override def write[T](ds: Dataset[T]): Write[T] = Write(ds)

  val queryTrait: Query = new Query {
    override def sql(query: String): Task[DataFrame] =
      Task(spark.sql(query))
  }

  override def query: Query = queryTrait

  override def ss: Task[SparkSession] = Task(spark)
}

object SparkEnv {

  trait Read {
    def option(key: String, value: String): Read

    def parquet(path: String): ZIO[SparkEnv, Throwable, DataFrame]

    def textFile(path: String): ZIO[SparkEnv, Throwable, DataFrame]
  }

  object implicits {

    implicit class DsOps[T](ds: Dataset[T]) {
      def zcache: Task[Dataset[T]] = Task(ds.cache)

      def zwrite: Write[T] = Write(ds)

    }

  }

  type TaskS[X] = TaskR[SparkEnv, X]

  def sql(queryString: String): TaskS[DataFrame] =
    ZIO.accessM(_.query.sql(queryString))

  def sparkSession: TaskS[SparkSession] =
    ZIO.accessM(_.ss)

  def read: Read = {
    case class ReadImpl(config: Seq[(String, String)]) extends Read {
      private def dataFrameReader: TaskS[DataFrameReader] =
        sparkSession.map(ss => config.foldLeft(ss.read)({ case (dfr, (k, v)) => dfr.option(k, v) }))

      override def option(key: String, value: String): SparkEnv.Read = copy(config :+ ((key, value)))

      override def parquet(path: String): TaskS[DataFrame] =
        dataFrameReader.map(dr => dr.parquet(path))

      override def textFile(path: String): TaskS[DataFrame] =
        dataFrameReader.map(dr => dr.textFile(path).toDF())
    }

    ReadImpl(Nil)
  }
}
