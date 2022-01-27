package zio.spark.sql

import org.apache.spark.sql.{SparkSession => UnderlyingSparkSession}

import zio._

final case class ZSparkSession(session: UnderlyingSparkSession) extends SparkSession {
  override def read: ZDataFrameReader = ZDataFrameReader(session.read)

  override def close: Task[Unit] = Task.attemptBlocking(session.close())
}

object ZSparkSession {
  def builder(): Builder = ZBuilder(UnderlyingSparkSession.builder())

  final case class ZBuilder(builder: UnderlyingSparkSession.Builder) extends Builder {

    import Builder._

    override def getOrCreateLayer(): ZLayer[Any, Throwable, SparkSession] =
      ZLayer.fromAcquireRelease(getOrCreate())(_.close.orDie)

    override def getOrCreate(): Task[SparkSession] = Task.attemptBlocking(ZSparkSession(builder.getOrCreate()))

    override def master(masterConfiguration: MasterConfiguration): Builder =
      master(masterConfigurationToMaster(masterConfiguration))

    override def master(master: String): Builder = ZBuilder(builder.master(master))

    override def appName(name: String): Builder = ZBuilder(builder.appName(name))

    def underlying: UnderlyingSparkSession.Builder = builder
  }
}
