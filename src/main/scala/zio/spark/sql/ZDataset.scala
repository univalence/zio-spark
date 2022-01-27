package zio.spark.sql

import org.apache.spark.sql.{Dataset => UnderlyingDataset}

import zio.Task

final case class ZDataset[T](ds: UnderlyingDataset[T]) extends Dataset[T] {
  override def limit(n: Int): Dataset[T] = transformation(_.limit(n))

  def transformation(f: UnderlyingDataset[T] => UnderlyingDataset[T]): Dataset[T] = ZDataset(f(ds))

  override def count(): Task[Long] = action(_.count())

  def action[A](f: UnderlyingDataset[T] => A): Task[A] = Task.attemptBlocking(f(ds))
}
