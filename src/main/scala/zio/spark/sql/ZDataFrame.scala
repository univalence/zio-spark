package zio.spark.sql
import org.apache.spark.sql.{DataFrame => UnderlyingDataFrame}

import zio.Task

final case class ZDataFrame(raw: UnderlyingDataFrame) extends DataFrame {
  override def limit(n: Int): DataFrame = transformation(_.limit(n))

  def transformation(f: UnderlyingDataFrame => UnderlyingDataFrame): DataFrame = ZDataFrame(f(raw))

  override def count(): Task[Long] = action(_.count())

  def action[A](f: UnderlyingDataFrame => A): Task[A] = Task.attemptBlocking(f(raw))
}
