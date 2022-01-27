package zio.spark.sql

import org.apache.spark.sql.{Dataset => UnderlyingDataset, Encoder}

import zio.Task

final case class ZDataset[T](ds: UnderlyingDataset[T]) extends Dataset[T] {
  override def as[U: Encoder]: Dataset[U] = transformation(_.as[U])

  def transformation[U](f: UnderlyingDataset[T] => UnderlyingDataset[U]): Dataset[U] = ZDataset(f(ds))

  override def limit(n: Int): Dataset[T] = transformation(_.limit(n))

  override def map[U: Encoder](f: T => U): Dataset[U] = transformation(_.map(f))

  override def count(): Task[Long] = action(_.count())

  def action[A](f: UnderlyingDataset[T] => A): Task[A] = Task.attemptBlocking(f(ds))

  override def collect(): Task[List[T]] = action(_.collect().toList)
}
