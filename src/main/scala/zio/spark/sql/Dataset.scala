package zio.spark.sql

import org.apache.spark.sql.Encoder

import zio._

trait Dataset[T] {
  def as[U: Encoder]: Dataset[U]
  def limit(n: Int): Dataset[T]
  def map[U: Encoder](f: T => U): Dataset[U]

  def count(): Task[Long]
  def collect(): Task[List[T]]
}
