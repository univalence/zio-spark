package zio.spark.sql

import org.apache.spark.sql.{SparkSession => UnderlyingSparkSession}

import zio._

final case class ZSparkSession(session: UnderlyingSparkSession) extends SparkSession {
  override def read: DataFrameReader = ZDataFrameReader(session.read)

  override def close: Task[Unit] = Task.attemptBlocking(session.close())
}

object ZSparkSession {
  def builder(): Builder = ZBuilder(UnderlyingSparkSession.builder())

  final case class ZBuilder(builder: UnderlyingSparkSession.Builder) extends Builder {

    import Builder._

    /** Create a layer containing the spark session. */
    override def getOrCreateLayer(): ZLayer[Any, Throwable, SparkSession] =
      ZLayer.fromAcquireRelease(getOrCreate())(_.close.orDie)

    /** Create the spark session. */
    override def getOrCreate(): Task[SparkSession] = Task.attemptBlocking(ZSparkSession(builder.getOrCreate()))

    /**
     * Configure the master of the spark session based on a
     * [[Builder.MasterConfiguration]].
     */
    override def master(masterConfiguration: MasterConfiguration): Builder =
      master(masterConfigurationToMaster(masterConfiguration))

    /**
     * Configure the master of the spark session based on the string
     * representation.
     */
    override def master(master: String): Builder = ZBuilder(builder.master(master))

    /** Configure the application name of the spark session. */
    override def appName(name: String): Builder = ZBuilder(builder.appName(name))
  }
}
