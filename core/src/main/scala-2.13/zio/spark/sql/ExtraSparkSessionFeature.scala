package zio.spark.sql

import org.apache.spark.sql.{Sniffer213, SparkSession => UnderlyingSparkSession}
import zio.Task
import zio.spark.internal.Impure
import zio.spark.internal.Impure.ImpureBox

abstract class ExtraSparkSessionFeature(underlyingSparkSession: ImpureBox[UnderlyingSparkSession])
  extends Impure(underlyingSparkSession) {
  def withActive[T](block: => T): Task[T] = attempt(Sniffer213.sparkSessionWithActive(_, block))
}