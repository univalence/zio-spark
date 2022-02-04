package zio.spark

import org.apache.spark.sql.{Row, SparkSession => UnderlyingSparkSession}

import zio._

package object sql extends SqlImplicits {
  type DataFrame = Dataset[Row]
  type Spark[A]  = RIO[SparkSession, A]

  /** Wrap an effecful spark job into zio-spark. */
  def fromSpark[Out](f: UnderlyingSparkSession => Out): Spark[Out] =
    for {
      ss     <- ZIO.service[SparkSession]
      output <- Task.attemptBlocking(f(ss.session))
    } yield output
}
