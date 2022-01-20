package zio.spark.sql

import zio._

trait SparkSession {
  def read: DataFrameReader

  def close: Task[Unit]
}