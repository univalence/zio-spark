package zio.spark.sql

import zio._

trait DataFrame {
  def limit(n: Int): DataFrame

  def count(): Task[Long]
}
