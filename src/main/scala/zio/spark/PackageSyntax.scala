package zio.spark

import zio.{ RIO, Task, ZIO, ZLayer }

import zio.spark.wrap.{ Clean, Impure, ImpureF }

import org.apache.spark.sql.{ DataFrameReader, SparkSession }

trait PackageSyntax {

  implicit class ZSparkContextF[-R](rio: RIO[R, ZSparkContext]) extends ImpureF(rio) {
    def textFile(path: String): RIO[R, ZRDD[String]] = execute(_.textFile(path))
  }

  final def sql(queryString: String): Spark[ZDataFrame] = ZIO.accessM(_.get.sql(queryString))

  final class ZReader(rio: Spark[Impure[DataFrameReader]]) extends ImpureF(rio) {
    private type V = DataFrameReader

    private def chain(f: V => V): ZReader = new ZReader(execute(f))

    def option(key: String, value: String): ZReader = chain(_.option(key, value))

    def format(source: String): ZReader = chain(_.format(source))

    def schema(schema: String): ZReader = chain(_.schema(schema))

    def parquet(path: String): Spark[ZDataFrame] = execute(_.parquet(path))

    def textFile(path: String): Spark[ZDataset[String]] = execute(_.textFile(path))

    def load(path: String): Spark[ZDataFrame] = execute(_.load(path))

    def load: Spark[ZDataFrame] = execute(_.load())

    def csv(path: String): Spark[ZDataFrame] = execute(_.csv(path))

  }

  final def read: ZReader = new ZReader(retroCompat(_.read))

  final def sparkSession: RIO[SparkEnv, ZSparkSession] = ZIO.access(_.get)

  final def sparkContext: RIO[SparkEnv, ZSparkContext] = sparkSession map (_.sparkContext)

  final class ZBuilder protected[spark] (rio: Task[Impure[SparkSession.Builder]]) extends ImpureF(rio) {
    private type V = SparkSession.Builder
    private def chain(f: V => V): ZBuilder = new ZBuilder(execute(f))

    def appName(name: String): ZBuilder              = chain(_.appName(name))
    def master(master: String): ZBuilder             = chain(_.master(master))
    def config(key: String, value: String): ZBuilder = chain(_.config(key, value))

    def getOrCreate: ZLayer[Any, Throwable, SparkEnv] =
      ZLayer.fromAcquireRelease(execute(_.getOrCreate()))(_.execute(_.close()).orDie)
  }

  final def builder: ZBuilder = new ZBuilder(wrapEffect(SparkSession.builder()))

  final def retroCompat[T, Pure](f: SparkSession => T)(implicit W: Clean.Aux[T, Pure]): RIO[SparkEnv, Pure] =
    sparkSession >>= (_.execute(f))

  final def wrapEffect[T](t: => T)(implicit W: Clean[T]): Task[W.Out] = Task(W(t))
}
