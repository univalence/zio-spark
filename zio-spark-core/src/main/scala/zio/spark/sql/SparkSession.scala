package zio.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, SparkSession => UnderlyingSparkSession}

import zio._
import zio.spark.parameter._
import zio.spark.sql.DataFrameReader.WithoutSchema
import zio.spark.sql.SparkSession.Conf
import zio.spark.sql.streaming.DataStreamReader

final case class SparkSession(underlyingSparkSession: UnderlyingSparkSession)
    extends ExtraSparkSessionFeature(underlyingSparkSession) {

  val sparkContext: TMP = TMP(underlyingSparkSession.sparkContext)

  /** Closes the current SparkSession. */
  def close(implicit trace: Trace): Task[Unit] = ZIO.attempt(underlyingSparkSession.close())

  /** Executes a SQL query using Spark. */
  def sql(sqlText: String)(implicit trace: Trace): Task[DataFrame] =
    ZIO.attempt(Dataset(underlyingSparkSession.sql(sqlText)))

  /** Creates a new [[Dataset]] of type T containing zero elements. */
  def emptyDataset[T: Encoder]: Dataset[T] = Dataset(underlyingSparkSession.emptyDataset[T])

  def conf: Conf =
    new Conf {
      override def getAll(implicit trace: Trace): UIO[Map[String, String]] =
        ZIO.succeed(underlyingSparkSession.conf.getAll)
    }
}

object SparkSession {
  trait Conf {
    def getAll(implicit trace: Trace): UIO[Map[String, String]]
  }

  /** Closes the current SparkSession. */
  def close(implicit trace: Trace): RIO[SparkSession, Unit] = ZIO.service[SparkSession].flatMap(_.close)

  /** Executes a SQL query using Spark. */
  def sql(sqlText: String)(implicit trace: Trace): RIO[SparkSession, DataFrame] =
    ZIO.service[SparkSession].flatMap(_.sql(sqlText))

  /** Creates a new [[Dataset]] of type T containing zero elements. */
  def emptyDataset[T: Encoder]: RIO[SparkSession, Dataset[T]] = ZIO.service[SparkSession].map(_.emptyDataset[T])

  def conf: URIO[SparkSession, Conf] = ZIO.service[SparkSession].map(_.conf)

  /** Creates a DataFrameReader. */
  def read: DataFrameReader[WithoutSchema] = DataFrameReader(Map.empty, None)

  /** Creates a DataStreamReader. */
  def readStream: DataStreamReader = DataStreamReader(Map.empty, None)

  /**
   * Creates a [[SparkSession.Builder]].
   *
   * See [[UnderlyingSparkSession.builder]] for more information.
   */
  def builder: Builder = Builder(UnderlyingSparkSession.builder(), Map.empty)

  def attempt[Out](f: UnderlyingSparkSession => Out)(implicit trace: Trace): SIO[Out] =
    ZIO.serviceWithZIO[SparkSession](ss => ZIO.attempt(f(ss.underlyingSparkSession)))

  final case class Builder(
      builder:      UnderlyingSparkSession.Builder,
      extraConfigs: Map[String, String],
      hiveSupport:  Boolean = false
  ) {
    self =>

    /**
     * Transforms the creation of the SparkSession into a managed layer
     * that will open and close the SparkSession when the job is done.
     */
    def asLayer: ZLayer[Any, Throwable, SparkSession] = ZLayer.scoped(acquireRelease)

    private def construct: UnderlyingSparkSession.Builder = {
      val configuredBuilder: UnderlyingSparkSession.Builder =
        extraConfigs.foldLeft(builder) { case (oldBuilder, (configKey, configValue)) =>
          oldBuilder.config(configKey, configValue)
        }

      if (hiveSupport) configuredBuilder.enableHiveSupport()
      else configuredBuilder
    }

    /**
     * Unsafely get or create a SparkSession without ensuring that the
     * session will be closed.
     *
     * See [[UnderlyingSparkSession.Builder.getOrCreate]] for more
     * information.
     */
    def getOrCreate(implicit trace: Trace): Task[SparkSession] = ZIO.attempt(SparkSession(self.construct.getOrCreate()))

    /**
     * Tries to create a spark session.
     *
     * See [[UnderlyingSparkSession.Builder.getOrCreate]] for more
     * information.
     */
    def acquireRelease(implicit trace: Trace): ZIO[Scope, Throwable, SparkSession] =
      ZIO.acquireRelease(getOrCreate)(ss => ZIO.attempt(ss.close).orDie)

    /** Adds a spark configuration to the Builder. */
    def config(conf: SparkConf): Builder = configs(conf.getAll.toMap)

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

    /**
     * Enables Hive support, including connectivity to a persistent Hive
     * metastore, support for Hive serdes, and Hive user-defined
     * functions.
     *
     * @since 2.0.0
     */
    def enableHiveSupport: Builder = copy(hiveSupport = true)
  }
}
