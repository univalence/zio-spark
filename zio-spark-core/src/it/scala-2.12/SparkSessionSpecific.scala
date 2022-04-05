import org.apache.spark.sql.Sniffer212

import zio.{Task, ZTraceElement}

final case class SparkSessionSpecific(self: SparkSession) {
  def withActive[T](block: => T)(implicit trace: ZTraceElement): Task[T] =
    Task.attempt(Sniffer212.sparkSessionWithActive(self.underlying, block))
}
