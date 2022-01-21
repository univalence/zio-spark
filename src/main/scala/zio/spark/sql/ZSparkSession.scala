package zio.spark.sql

import org.apache.spark.sql.{SparkSession => UnderlyingSparkSession}

import zio._

case class ZSparkSession(session: UnderlyingSparkSession) extends SparkSession {
  override def read: DataFrameReader = ZDataFrameReader(session.read)

  override def close: Task[Unit] = Task.attemptBlocking(session.close())
}

object ZSparkSession {
  def builder(): Builder = ZBuilder(UnderlyingSparkSession.builder())

  case class ZBuilder(builder: UnderlyingSparkSession.Builder) extends Builder {

    override def getOrCreateLayer(): ZLayer[Any, Throwable, SparkSession] = {
      val acquire = Task.attempt(getOrCreate())
      ZLayer.fromAcquireRelease(acquire)(_.close.orDie)
    }

    override def getOrCreate(): SparkSession = ZSparkSession(builder.getOrCreate())

    override def master(builderMaster: Builder.MasterConfiguration): Builder = {
      import Builder._

      val stringifyMaster =
        builderMaster match {
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
      master(stringifyMaster)
    }

    override def master(master: String): Builder = ZBuilder(builder.master(master))

    override def appName(name: String): Builder = ZBuilder(builder.appName(name))
  }
}
