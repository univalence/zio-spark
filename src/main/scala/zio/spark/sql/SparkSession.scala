package zio.spark.sql

import org.apache.spark.sql.{SparkSession => UnderlyingSparkSession}

import zio._

trait SparkSession {
  def read: DataFrameReader

  def close: Task[Unit]
}

object SparkSession extends Accessible[SparkSession] {
  def builder(): Builder = Builder(UnderlyingSparkSession.builder())

  final case class Builder(builder: UnderlyingSparkSession.Builder) {

    import Builder._

    def getOrCreateLayer(): ZLayer[Any, Throwable, SparkSession] =
      ZLayer.fromAcquireRelease(getOrCreate())(_.close.orDie)

    def getOrCreate(): Task[SparkSession] = Task.attemptBlocking(SparkSessionLive(builder.getOrCreate()))

    def master(masterConfiguration: MasterConfiguration): Builder =
      master(masterConfigurationToMaster(masterConfiguration))

    def master(master: String): Builder = Builder(builder.master(master))

    def appName(name: String): Builder = Builder(builder.appName(name))
  }

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

    final case class MasterNodeConfiguration(host: String, port: Int) {
      def toSparkString: String = s"$host:$port"
    }

    final case class Local(nWorkers: Int) extends MasterConfiguration

    final case class LocalWithFailures(nWorkers: Int, maxFailures: Int) extends MasterConfiguration

    final case class LocalAllNodesWithFailures(maxFailures: Int) extends MasterConfiguration

    final case class Spark(masters: List[MasterNodeConfiguration]) extends MasterConfiguration

    final case class Mesos(master: MasterNodeConfiguration) extends MasterConfiguration

    case object LocalAllNodes extends MasterConfiguration

    case object Yarn extends MasterConfiguration
  }

}

final case class SparkSessionLive(session: UnderlyingSparkSession) extends SparkSession {
  def read: DataFrameReader = DataFrameReader(session.read)

  def close: Task[Unit] = Task.attemptBlocking(session.close())
}
