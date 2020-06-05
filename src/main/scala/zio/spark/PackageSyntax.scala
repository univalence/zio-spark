package zio.spark

import org.apache.spark.sql.{ DataFrameReader, SparkSession }
import zio.spark.wrap.{ Wrap, ZWrap, ZWrapLazy }
import zio.{ Has, RIO, Task, ZIO, ZLayer }

trait PackageSyntax {

  def sql(queryString: String): SIO[ZDataFrame] = ZIO.accessM(_.get.sql(queryString))

  class Read(rio: SIO[ZWrap[DataFrameReader]]) extends ZWrapLazy(rio) {
    private val chain = makeChain(new Read(_))

    def option(key: String, value: String): Read = chain(_.option(key, value))
    def format(source: String): Read             = chain(_.format(source))
    def schema(schema: String): Read             = chain(_.schema(schema))

    def parquet(path: String): SIO[ZDataFrame]        = execute(_.parquet(path))
    def textFile(path: String): SIO[ZDataset[String]] = execute(_.textFile(path))
    def load(path: String): SIO[ZDataFrame]           = execute(_.load(path))
  }

  def read: Read = new Read(retroCompat(_.read))

  def sparkSession: SIO[ZSparkSession] = ZIO.access(_.get)

  class Builder(rio: Task[ZWrap[SparkSession.Builder]]) extends ZWrapLazy(rio) {
    private val chain = makeChain(new Builder(_))

    def appName(name: String): Builder              = chain(_.appName(name))
    def master(master: String): Builder             = chain(_.master(master))
    def config(key: String, value: String): Builder = chain(_.config(key, value))

    def getOrCreate: ZLayer[Any, Throwable, SparkEnv] =
      ZLayer.fromAcquireRelease(execute(_.getOrCreate()))(_.execute(_.close()).orDie)
  }

  def builder: Builder = new Builder(wrapEffect(SparkSession.builder()))

  def retroCompat[T, Pure](f: SparkSession => T)(implicit W: Wrap.Aux[T, Pure]): SIO[Pure] =
    sparkSession >>= (_.execute(f))

  def wrapEffect[T](t: => T)(implicit W: Wrap[T]): Task[W.Out] = Task(W(t))
}
