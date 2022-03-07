package zio.spark.sql

import org.apache.spark.sql.{SparkSession => UnderlyingSparkSession}

import zio._
import zio.spark.internal.Impure.ImpureBox
import zio.spark.parameter._
import zio.spark.sql.SparkSession.Conf

final case class SparkSession(underlyingSparkSession: ImpureBox[UnderlyingSparkSession])
    extends ExtraSparkSessionFeature(underlyingSparkSession) {
  import underlyingSparkSession._

  /** Closes the current SparkSession. */
  def close: Task[Unit] = attemptBlocking(_.close())

  /** Executes a SQL query using Spark. */
  def sql(sqlText: String): Task[DataFrame] = attemptBlocking(ss => Dataset(ss.sql(sqlText)))

  def conf: Conf =
    new Conf {
      override def getAll: UIO[Map[String, String]] = succeed(_.conf.getAll)
    }
}

object SparkSession extends Accessible[SparkSession] {

  def apply(underlyingSparkSession: UnderlyingSparkSession): SparkSession =
    SparkSession(ImpureBox(underlyingSparkSession))

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

  def attempt[Out](f: UnderlyingSparkSession => Out): Spark[Out] = ZIO.serviceWithZIO[SparkSession](_.attempt(f))

  final case class Builder(builder: UnderlyingSparkSession.Builder, extraConfigs: Map[String, String]) {
    self =>

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
    def getOrCreate: Task[SparkSession] = {
      val builderConfigured =
        extraConfigs.foldLeft(builder) { case (oldBuilder, (configKey, configValue)) =>
          oldBuilder.config(configKey, configValue)
        }

      Task.attemptBlocking(SparkSession(builderConfigured.getOrCreate()))
    }

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
