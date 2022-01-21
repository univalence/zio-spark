package zio.spark.sql

import zio._

object Builder {
  def masterConfigurationToMaster(masterConfiguration: MasterConfiguration): String =
    masterConfiguration match {
      case Local(nWorkers)                          => s"local[$nWorkers]"
      case LocalWithFailures(nWorkers, maxFailures) => s"local[$nWorkers,$maxFailures]"
      case LocalAllNodes                            => "local[*]"
      case LocalAllNodesWithFailures(maxFailures)   => s"local[*,$maxFailures]"
      case Spark(masters) =>
        val masterUrls = masters.map(_.toSparkString).mkString(",")
        s"spark://$masterUrls"
      case Mesos(master) => s"mesos://${master.toSparkString}"
      case Yarn          => "yarn"
    }

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
  import Builder._

  def getOrCreate(): SparkSession

  def getOrCreateLayer(): ZLayer[Any, Throwable, SparkSession]

  def master(masterConfiguration: MasterConfiguration): Builder

  def master(master: String): Builder

  def appName(name: String): Builder
}
