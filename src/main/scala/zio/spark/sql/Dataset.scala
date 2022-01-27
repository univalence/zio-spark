package zio.spark.sql

import zio._

trait Dataset[T] {
  def limit(n: Int): Dataset[T]

  def count(): Task[Long]
}
