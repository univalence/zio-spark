package zio.spark.sql

import org.apache.spark.sql.{Dataset => UnderlyingDataset, Encoder}

import zio.Task

final case class Dataset[T](ds: UnderlyingDataset[T]) {
  def as[U: Encoder]: Dataset[U] = transformation(_.as[U])

  def limit(n: Int): Dataset[T] = transformation(_.limit(n))

  def map[U: Encoder](f: T => U): Dataset[U] = transformation(_.map(f))

  def transformation[U](f: UnderlyingDataset[T] => UnderlyingDataset[U]): Dataset[U] = new Dataset(f(ds))

  def count(): Task[Long] = action(_.count())

  def action[A](f: UnderlyingDataset[T] => A): Task[A] = Task.attemptBlocking(f(ds))

  def collect(): Task[List[T]] = action(_.collect().toList)
}
