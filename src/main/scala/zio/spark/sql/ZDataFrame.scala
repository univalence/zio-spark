package zio.spark.sql
import org.apache.spark.sql.{DataFrame => UnderlyingDataFrame}

import zio.Task

final case class ZDataFrame(raw: UnderlyingDataFrame) extends DataFrame {
  def transformation(f: UnderlyingDataFrame => UnderlyingDataFrame): DataFrame = ZDataFrame(f(raw))

  def action[A](f: UnderlyingDataFrame => A): Task[A] = Task.attempt(f(raw))

  override def limit(n: Int): DataFrame = transformation(_.limit(n))

  override def count(): Task[Long] = action(_.count())
}