package zio.spark.sql

import zio._

object Builder {
  sealed trait MasterConfiguration

  case class MasterNodeConfiguration(host: String, port: Int) {
    def toSparkString: String = s"$host:$port"
  }

  case class Local(nWorkers: Int) extends MasterConfiguration

  case class LocalWithFailures(nWorkers: Int, maxFailures: Int) extends MasterConfiguration

  case class LocalAllNodesWithFailures(maxFailures: Int) extends MasterConfiguration

  case class Spark(masters: List[MasterNodeConfiguration]) extends MasterConfiguration

  case class Mesos(master: MasterNodeConfiguration) extends MasterConfiguration

  case object LocalAllNodes extends MasterConfiguration

  case object Yarn extends MasterConfiguration
}

trait Builder {
  def getOrCreate(): SparkSession

  def getOrCreateLayer(): ZLayer[Any, Throwable, SparkSession]

  def master(masterConfiguration: Builder.MasterConfiguration): Builder

  def master(master: String): Builder

  def appName(name: String): Builder
}
