package zio.spark

import org.apache.spark.SparkContext
import org.apache.spark.sql.{ DataFrameReader, SparkSession }
import zio.spark.wrap.{ Wrap, ZWrap, ZWrapF, ZWrapFImpure }
import zio.{ Has, RIO, Task, URIO, ZIO, ZLayer }

trait PackageSyntax {

  implicit class _ZSparkContextF[-R](rio: RIO[R, ZSparkContext]) {

    def textFile(path: String): RIO[R, ZRDD[String]] = rio >>= (_.textFile(path))
  }

  final def sql(queryString: String): SIO[ZDataFrame] = ZIO.accessM(_.get.sql(queryString))

  final class Read(rio: SIO[ZWrap[DataFrameReader]]) extends ZWrapFImpure(rio) {
    private val chain = makeChain(new Read(_))

    def option(key: String, value: String): Read = chain(_.option(key, value))
    def format(source: String): Read             = chain(_.format(source))
    def schema(schema: String): Read             = chain(_.schema(schema))

    def parquet(path: String): SIO[ZDataFrame]        = execute(_.parquet(path))
    def textFile(path: String): SIO[ZDataset[String]] = execute(_.textFile(path))
    def load(path: String): SIO[ZDataFrame]           = execute(_.load(path))
  }

  final def read: Read = new Read(retroCompat(_.read))

  final def sparkSession: RIO[SparkEnv, ZSparkSession] = ZIO.access(_.get)

  final def sparkContext: RIO[SparkEnv, ZSparkContext] = sparkSession map (_.sparkContext)

  final class Builder(rio: Task[ZWrap[SparkSession.Builder]]) extends ZWrapFImpure(rio) {
    private val chain = makeChain(new Builder(_))

    def appName(name: String): Builder              = chain(_.appName(name))
    def master(master: String): Builder             = chain(_.master(master))
    def config(key: String, value: String): Builder = chain(_.config(key, value))

    def getOrCreate: ZLayer[Any, Throwable, SparkEnv] =
      ZLayer.fromAcquireRelease(execute(_.getOrCreate()))(_.execute(_.close()).orDie)
  }

  final def builder: Builder = new Builder(wrapEffect(SparkSession.builder()))

  final def retroCompat[T, Pure](f: SparkSession => T)(implicit W: Wrap.Aux[T, Pure]): RIO[SparkEnv, Pure] =
    sparkSession >>= (_.execute(f))

  final def wrapEffect[T](t: => T)(implicit W: Wrap[T]): Task[W.Out] = Task(W(t))
}
