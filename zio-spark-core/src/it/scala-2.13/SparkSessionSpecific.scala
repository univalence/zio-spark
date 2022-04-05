import org.apache.spark.sql.Sniffer213

import zio.{Task, ZTraceElement}

final case class SparkSessionSpecific(self: SparkSession) {
  def withActive[T](block: => T)(implicit trace: ZTraceElement): Task[T] =
    Task.attempt(Sniffer213.sparkSessionWithActive(self.underlying, block))
}
