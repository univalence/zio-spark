package zio.spark.sql

import zio._

trait Builder {
  def getOrCreate(): SparkSession
  def getOrCreateLayer(): ZLayer[Any, Throwable, SparkSession]

  def master(master: String): Builder
  def appName(name: String): Builder
}
