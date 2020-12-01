package zio

import org.apache.spark.sql.SparkSession
import zio.spark.wrap.Clean

package object spark extends PackageSyntax {
  type SparkEnv = Has[ZSparkSession]

  type Spark[T] = RIO[SparkEnv, T]

  final def sql(queryString: String): Spark[ZDataFrame] = sparkSession >>= (_.sql(queryString))

  final def sparkSession: RIO[SparkEnv, ZSparkSession] = ZIO.access(_.get)

  final def sparkContext: RIO[SparkEnv, ZSparkContext] = sparkSession map (_.sparkContext)

  final def builder: ZBuilder = new ZBuilder(wrapEffect(SparkSession.builder()))

  final def read: ZReader = new ZReader(retroCompat(_.read))

  final def retroCompat[T, Pure](f: SparkSession => T)(implicit W: Clean.Aux[T, Pure]): RIO[SparkEnv, Pure] =
    sparkSession >>= (_.execute(f))

  final def wrapEffect[T](t: => T)(implicit W: Clean[T]): Task[W.Out] = Task(W(t))
}
