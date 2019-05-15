package io.univalence.sparkzio

import org.apache.spark.sql.{ DataFrame, Dataset, SparkSession }
import scalaz.zio.{ DefaultRuntime, IO, Task, TaskR, ZIO }

class Write[T](ds: Dataset[T], options: Seq[(String, String)]) {
  def option(key: String, value: String): Write[T] = new Write(ds, options :+ (key -> value))

  def text(path: String): Task[Unit] = Task.effect(ds.write.options(options.toMap).text(path))

  def parquet(path: String): Task[Unit] = Task.effect(ds.write.options(options.toMap).parquet(path))

  def cache: Task[Unit] = Task.effect(ds.cache)
}

trait SparkEnv {

  trait Read {
    def option(key: String, value: String): Read

    def parquet(path: String): Task[DataFrame]

    def textFile(path: String): Task[DataFrame]
  }

  def read: Read

  def write[T](ds: Dataset[T]): Write[T] = new Write(ds, Seq.empty)

  trait Query {
    def sql(query: String): Task[DataFrame]
  }

  def query: Query

  def ss: Task[SparkSession]
}

class SparkZIO(spark: SparkSession) extends SparkEnv {

  val readTrait: Read = new Read {
    override def option(key: String, value: String): Read =
      ???

    override def parquet(path: String): Task[DataFrame] =
      Task.effect(spark.read.parquet(path))

    override def textFile(path: String): Task[DataFrame] =
      Task.effect(spark.read.textFile(path).toDF())
  }

  override def read: Read = readTrait

  override def write[T](ds: Dataset[T]): Write[T] = new Write(ds, Seq.empty)

  val queryTrait: Query = new Query {
    override def sql(query: String): Task[DataFrame] =
      Task.effect(spark.sql(query))
  }

  override def query: Query = queryTrait

  override def ss: Task[SparkSession] = Task.effect(spark)
}

object SparkEnv {

  object implicits {

    implicit class DsOps[T](ds: Dataset[T]) {
      def zcache: Task[Dataset[T]] = Task.effect(ds.cache)

      def zwrite: Write[T] = new Write(ds, Seq.empty)

    }

  }

  type TaskS[X] = TaskR[SparkEnv, X]

  def sql(query: String): TaskS[DataFrame] =
    ZIO.accessM(_.query.sql(query))

  def sparkSession: TaskS[SparkSession] = {
    ZIO.accessM(_.ss)
  }
}

//TODO Move to Test
object SparkTest {

  def main(args: Array[String]): Unit = {

    val runtime: DefaultRuntime = new DefaultRuntime {}
    val sparkEnv                = new SparkZIO(SparkSession.builder.master("local[*]").getOrCreate())

    import SparkEnv.implicits._

    val programWrite: ZIO[SparkEnv, Throwable, DataFrame] = for {
      df <- sparkEnv.read.textFile("tata")
      _  <- df.zwrite.text("tata3")
    } yield df

    val liveProgramWrite: IO[Throwable, DataFrame] = programWrite.provide(sparkEnv)

    val ya: DataFrame = runtime.unsafeRun(liveProgramWrite)
    ya.show()
  }
}
