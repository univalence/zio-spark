package zio.spark.sql

import org.apache.spark.sql.{SparkSession => UnderlyingSparkSession}

import zio._

final case class ZSparkSession(session: UnderlyingSparkSession) extends SparkSession {
  override def read: ZDataFrameReader = ZDataFrameReader(session.read)

  override def close: Task[Unit] = Task.attemptBlocking(session.close())
}

object ZSparkSession {
  def builder(): ZBuilder = ZBuilder(UnderlyingSparkSession.builder())

  final case class ZBuilder(builder: UnderlyingSparkSession.Builder) extends Builder {

    import Builder._

    override def getOrCreateLayer(): ZLayer[Any, Throwable, ZSparkSession] =
      ZLayer.fromAcquireRelease(getOrCreate())(_.close.orDie)

    override def getOrCreate(): Task[ZSparkSession] = Task.attemptBlocking(ZSparkSession(builder.getOrCreate()))

    override def master(masterConfiguration: MasterConfiguration): ZBuilder =
      master(masterConfigurationToMaster(masterConfiguration))

    override def master(master: String): ZBuilder = ZBuilder(builder.master(master))

    override def appName(name: String): ZBuilder = ZBuilder(builder.appName(name))

    def underlying: UnderlyingSparkSession.Builder = builder
  }
}
