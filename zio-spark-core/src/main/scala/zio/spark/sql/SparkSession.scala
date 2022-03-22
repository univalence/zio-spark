package zio.spark.sql

import org.apache.spark.sql.{SparkSession => UnderlyingSparkSession}

import zio._
import zio.spark.parameter._
import zio.spark.sql.SparkSession.Conf

final case class SparkSession(underlyingSparkSession: UnderlyingSparkSession)
    extends ExtraSparkSessionFeature(underlyingSparkSession) {

  /** Closes the current SparkSession. */
  def close: Task[Unit] = ZIO.attemptBlocking(underlyingSparkSession.close())

  /** Executes a SQL query using Spark. */
  def sql(sqlText: String): Task[DataFrame] = ZIO.attemptBlocking(Dataset(underlyingSparkSession.sql(sqlText)))

  def conf: Conf =
    new Conf {
      override def getAll: UIO[Map[String, String]] = UIO.succeed(underlyingSparkSession.conf.getAll)
    }
}

object SparkSession extends Accessible[SparkSession] {
  trait Conf {
    def getAll: UIO[Map[String, String]]
  }

  /** Creates the DataFrameReader. */
  def read: DataFrameReader = DataFrameReader(Map.empty)

  /**
   * Creates a [[SparkSession.Builder]].
   *
   * See [[UnderlyingSparkSession.builder]] for more information.
   */
  def builder: Builder = Builder(UnderlyingSparkSession.builder(), Map.empty)

  def attempt[Out](f: UnderlyingSparkSession => Out): SIO[Out] =
    ZIO.serviceWithZIO[SparkSession](ss => ZIO.attempt(f(ss.underlyingSparkSession)))

  final case class Builder(builder: UnderlyingSparkSession.Builder, extraConfigs: Map[String, String]) {
    self =>

    /**
     * Transforms the creation of the SparkSession into a managed layer
     * that will open and close the SparkSession when the job is done.
     */
    def asLayer: ZLayer[Any, Throwable, SparkSession] = ZLayer.scoped(acquireRelease)

    private def construct: UnderlyingSparkSession.Builder =
      extraConfigs.foldLeft(builder) { case (oldBuilder, (configKey, configValue)) =>
        oldBuilder.config(configKey, configValue)
      }

    /**
     * Unsafely get or create a SparkSession without ensuring that the
     * session will be closed.
     *
     * See [[UnderlyingSparkSession.Builder.getOrCreate]] for more
     * information.
     */
    def getOrCreate: Task[SparkSession] = Task.attempt(SparkSession(self.construct.getOrCreate()))

    /**
     * Tries to create a spark session.
     *
     * See [[UnderlyingSparkSession.Builder.getOrCreate]] for more
     * information.
     */
    def acquireRelease: ZIO[Scope, Throwable, SparkSession] =
      ZIO.acquireRelease(getOrCreate)(ss => Task.attempt(ss.close).orDie)

    /** Adds multiple configurations to the Builder. */
    def configs(configs: Map[String, String]): Builder = copy(builder, extraConfigs ++ configs)

    /** Configures the master using a [[Master]]. */
    def master(masterMode: Master): Builder = master(masterMode.toString)

    /** Configures the master using a String. */
    def master(master: String): Builder = config("spark.master", master)

    /** Adds a config to the Builder. */
    def config(key: String, value: String): Builder = copy(builder, extraConfigs + (key -> value))

    /** Configures the application name. */
    def appName(name: String): Builder = config("spark.app.name", name)

    /** Adds an option to the Builder (for Int). */
    def config(key: String, value: Int): Builder = config(key, value.toString)

    /** Adds an option to the Builder (for Float). */
    def config(key: String, value: Float): Builder = config(key, value.toString)

    /** Adds an option to the Builder (for Double). */
    def config(key: String, value: Double): Builder = config(key, value.toString)

    /** Adds an option to the Builder (for Boolean). */
    def config(key: String, value: Boolean): Builder = config(key, value.toString)

    /**
     * Configure the amount of memory to use for the driver process
     * using a String.
     *
     * Note: In client mode, set this through the --driver-memory
     * command line option or in your default properties file.
     */
    def driverMemory(size: Size): Builder = driverMemory(size.toString)

    /**
     * Configure the amount of memory to use for the driver process
     * using a String.
     *
     * Note: In client mode, set this through the --driver-memory
     * command line option or in your default properties file.
     */
    def driverMemory(size: String): Builder = config("spark.driver.memory", size)
  }
}
