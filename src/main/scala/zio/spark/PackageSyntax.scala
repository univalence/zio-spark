package zio.spark

import org.apache.spark.sql.{ DataFrameReader, SparkSession }
import zio.spark.wrap.{ Wrap, ZWrap, ZWrapLazyR }
import zio.{ Has, RIO, Task, ZIO, ZLayer }

trait PackageSyntax {

  def sql(queryString: String): SIO[ZDataFrame] = ZIO.accessM(_.get.sql(queryString))

  trait Read extends ZWrapLazyR[DataFrameReader, Read, SparkEnv] {
    def option(key: String, value: String): Read = chain(_.option(key, value))
    def format(source: String): Read             = chain(_.format(source))
    def schema(schema: String): Read             = chain(_.schema(schema))

    def parquet(path: String): SIO[ZDataFrame]        = execute(_.parquet(path))
    def textFile(path: String): SIO[ZDataset[String]] = execute(_.textFile(path))
    def load(path: String): SIO[ZDataFrame]           = execute(_.load(path))
  }

  def read: Read = {
    def create(task: SIO[ZWrap[DataFrameReader]]): Read =
      new Read {
        override protected def _task: RIO[SparkEnv, ZWrap[DataFrameReader]] = task
        override protected val copy: Copy                                   = create
      }

    create(sparkSession >>= (_.execute(_.read)))
  }

  def sparkSession: SIO[ZSparkSession] = ZIO.access(_.get)

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
