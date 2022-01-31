package zio.spark.sql

import org.apache.spark.sql.{SparkSession => UnderlyingSparkSession}

import zio._

final case class SparkSession(session: UnderlyingSparkSession) {

  /** Creates the DataFrameReader. */
  def read: DataFrameReader = DataFrameReader(session.read)

  /** Closes the current SparkSession. */
  def close: Task[Unit] = Task.attemptBlocking(session.close())
}

object SparkSession extends Accessible[SparkSession] {

  /**
   * Creates a [[SparkSession.Builder]].
   *
   * See [[UnderlyingSparkSession.builder]] for more information.
   */
  def builder: Builder = Builder(UnderlyingSparkSession.builder())

  final case class Builder(builder: UnderlyingSparkSession.Builder) {

    import Builder._

    /**
     * Transforms the creation of the SparkSession into a managed layer
     * that will open and close the SparkSession when the job is done.
     */
    def getOrCreateLayer: ZLayer[Any, Throwable, SparkSession] = ZLayer.fromAcquireRelease(getOrCreate)(_.close.orDie)

    /**
     * Tries to create a spark session.
     *
     * See [[UnderlyingSparkSession.Builder.getOrCreate]] for more
     * information.
     */
    def getOrCreate: Task[SparkSession] = Task.attemptBlocking(SparkSession(builder.getOrCreate()))

    /**
     * Configures the master using a [[Builder.MasterConfiguration]].
     */
    def master(masterConfiguration: MasterConfiguration): Builder =
      master(masterConfigurationToMaster(masterConfiguration))

    /** Configures the master using a String. */
    def master(master: String): Builder = Builder(builder.master(master))

    /** Configures the application name. */
    def appName(name: String): Builder = Builder(builder.appName(name))
  }

  object Builder {

    /**
     * Converts the [[Builder.MasterConfiguration]] into its String
     * representation.
     */
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
