package zio.spark.sql

import org.apache.spark.sql.{SparkSession => UnderlyingSparkSession}
import zio._

case class ZSparkSession(session: UnderlyingSparkSession) extends SparkSession {
  override def read: DataFrameReader = ZDataFrameReader(session.read)

  override def close: Task[Unit] = Task.attemptBlocking(session.close())
}

object ZSparkSession {
  case class ZBuilder(builder: UnderlyingSparkSession.Builder) extends Builder {

    override def getOrCreate: SparkSession = ZSparkSession(builder.getOrCreate())

    override def getOrCreateLayer: ZLayer[Any, Throwable, SparkSession] = {
      val acquire = Task.attempt(getOrCreate)
      ZLayer.fromAcquireRelease(acquire)(_.close.orDie)
    }

    override def master(master: String): Builder = ZBuilder(builder.master(master))

    override def appName(name: String): Builder = ZBuilder(builder.appName(name))
  }

  def builder(): Builder = ZBuilder(UnderlyingSparkSession.builder())
}

