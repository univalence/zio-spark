package zio.spark

import org.apache.spark.sql.SparkSession
import zio.spark.wrap.{ Wrap, ZWrap }
import zio.{ Has, Task, ZIO, ZLayer }

trait PackageSyntax {

  def sql(queryString: String): SIO[ZDataFrame] = ZIO.accessM(_.get.sql(queryString))

  def read: Read = {
    case class ReadImpl(read: SIO[ZSparkSession#Read]) extends Read {
      override def option(key: String, value: String): Read      = copy(read.map(_.option(key, value)))
      override def parquet(path: String): SIO[ZDataFrame]        = read.flatMap(_.parquet(path))
      override def textFile(path: String): SIO[ZDataset[String]] = read.flatMap(_.textFile(path))
    }

    ReadImpl(sparkSession.map(_.read))
  }

  def sparkSession: SIO[ZSparkSession] = ZIO.access(_.get)

  trait Read {
    def option(key: String, value: String): Read
    def parquet(path: String): SIO[ZDataFrame]
    def textFile(path: String): SIO[ZDataset[String]]
  }

  trait Builder {
    def appName(name: String): Builder

    def master(master: String): Builder

    def config(key: String, value: String): Builder

    def getOrCreate: ZLayer[Any, Throwable, SparkEnv]
  }

  def builder: Builder = {
    case class BuilderImpl(builder: Task[ZWrap[SparkSession.Builder]]) extends Builder {

      def build(f: SparkSession.Builder => SparkSession.Builder): Builder =
        BuilderImpl(builder.flatMap(_.execute(f)))

      override def appName(name: String): Builder = build(_.appName(name))

      override def master(master: String): Builder = build(_.master(master))

      override def config(key: String, value: String): Builder = build(_.config(key, value))

      override def getOrCreate: ZLayer[Any, Throwable, Has[ZSparkSession]] =
        ZLayer.fromAcquireRelease(builder.flatMap(_.execute(_.getOrCreate())))(_.execute(_.close()).orDie)
    }

    BuilderImpl(Task(Wrap(SparkSession.builder())))

  }
}
