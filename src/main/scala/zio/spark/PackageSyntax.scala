package zio.spark

import org.apache.spark.SparkContext
import zio.{ RIO, Task, ZIO, ZLayer }
import zio.spark.wrap.{ Clean, Impure, ImpureF }
import org.apache.spark.sql.{ DataFrameReader, SparkSession }

trait PackageSyntax {

  implicit class ZSparkContextF[-R](rio: RIO[R, ZSparkContext]) {
    def textFile(path: String): RIO[R, ZRDD[String]] = rio >>= (_.execute(_.textFile(path)))
  }

  implicit final class ZReader(rio: Spark[Impure[DataFrameReader]]) extends ImpureF(rio) {
    override protected def copy(f: DataFrameReader => DataFrameReader): ZReader = new ZReader(execute(f))

    def option(key: String, value: String): ZReader = copy(_.option(key, value))
    def format(source: String): ZReader             = copy(_.format(source))
    def schema(schema: String): ZReader             = copy(_.schema(schema))

    def parquet(path: String): Spark[ZDataFrame]        = execute(_.parquet(path))
    def textFile(path: String): Spark[ZDataset[String]] = execute(_.textFile(path))
    def load(path: String): Spark[ZDataFrame]           = execute(_.load(path))
    def load: Spark[ZDataFrame]                         = execute(_.load())
    def csv(path: String): Spark[ZDataFrame]            = execute(_.csv(path))

  }

  implicit class ZBuilder protected[spark] (task: Task[Impure[SparkSession.Builder]]) extends ImpureF(task) {

    override protected def copy(f: SparkSession.Builder => SparkSession.Builder): ZBuilder = execute(f)

    def appName(name: String): ZBuilder               = copy(_.appName(name))
    def master(master: String): ZBuilder              = copy(_.master(master))
    def config(key: String, value: String): ZBuilder  = copy(_.config(key, value))
    def config(key: String, value: Long): ZBuilder    = copy(_.config(key, value))
    def config(key: String, value: Double): ZBuilder  = copy(_.config(key, value))
    def config(key: String, value: Boolean): ZBuilder = copy(_.config(key, value))
    def enableHiveSupport: ZBuilder                   = copy(_.enableHiveSupport())

    def getOrCreate: ZLayer[Any, Throwable, SparkEnv] =
      ZLayer.fromAcquireRelease(execute(_.getOrCreate()))(_.execute(_.close()).orDie)

  }

}
