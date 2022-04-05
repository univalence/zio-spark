/**
 * /!\ Warning /!\
 *
 * This file is generated using zio-spark-codegen, you should not edit
 * this file directly.
 */
 
package zio.spark.sql

import org.apache.spark.sql.Sniffer213
import zio.{Task, ZTraceElement}
import org.apache.spark.sql.{SparkSession => UnderlyingSparkSession}
import zio._import zio.spark.parameter._import zio.spark.sql._import zio.spark.sql.SparkSession.Conffinal case class SparkSession(underlying: UnderlyingSparkSession) { self =>
  def close(implicit trace: ZTraceElement): Task[Unit] = ZIO.attemptBlocking(underlying.close())
  def sql(sqlText: String)(implicit trace: ZTraceElement): Task[DataFrame] = ZIO.attemptBlocking(Dataset(underlying.sql(sqlText)))
  def conf: Conf = new Conf { override def getAll: UIO[Map[String, String]] = UIO.succeed(underlying.conf.getAll) }
  def withActive[T](block: => T)(implicit trace: ZTraceElement): Task[T] = Task.attempt(Sniffer213.sparkSessionWithActive(self.underlying, block))
}object SparkSession extends Accessible[SparkSession] {
  trait Conf { def getAll: UIO[Map[String, String]] }
  def read: DataFrameReader = DataFrameReader(Map.empty)
  def builder: Builder = Builder(UnderlyingSparkSession.builder(), Map.empty)
  def attempt[Out](f: UnderlyingSparkSession => Out)(implicit trace: ZTraceElement): SIO[Out] = ZIO.serviceWithZIO[SparkSession](ss => ZIO.attempt(f(ss.underlying))(trace))
  final case class Builder(builder: UnderlyingSparkSession.Builder, extraConfigs: Map[String, String], hiveSupport: Boolean = false) { self =>
    def asLayer: ZLayer[Any, Throwable, SparkSession] = ZLayer.scoped(acquireRelease)
    private def construct: UnderlyingSparkSession.Builder = {
      val configuredBuilder: UnderlyingSparkSession.Builder = extraConfigs.foldLeft(builder)({
        case (oldBuilder, (configKey, configValue)) =>
          oldBuilder.config(configKey, configValue)
      })
      if (hiveSupport) configuredBuilder.enableHiveSupport() else configuredBuilder
    }
    def getOrCreate(implicit trace: ZTraceElement): Task[SparkSession] = Task.attempt(SparkSession(self.construct.getOrCreate()))
    def acquireRelease(implicit trace: ZTraceElement): ZIO[Scope, Throwable, SparkSession] = ZIO.acquireRelease(getOrCreate)(ss => Task.attempt(ss.close).orDie)
    def configs(configs: Map[String, String]): Builder = copy(builder, extraConfigs ++ configs)
    def master(masterMode: Master): Builder = master(masterMode.toString)
    def master(master: String): Builder = config("spark.master", master)
    def config(key: String, value: String): Builder = copy(builder, extraConfigs + (key -> value))
    def appName(name: String): Builder = config("spark.app.name", name)
    def config(key: String, value: Int): Builder = config(key, value.toString)
    def config(key: String, value: Float): Builder = config(key, value.toString)
    def config(key: String, value: Double): Builder = config(key, value.toString)
    def config(key: String, value: Boolean): Builder = config(key, value.toString)
    def driverMemory(size: Size): Builder = driverMemory(size.toString)
    def driverMemory(size: String): Builder = config("spark.driver.memory", size)
    def enableHiveSupport: Builder = copy(hiveSupport = true)
  }
}