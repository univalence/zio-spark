package zio.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Row, SparkSession => UnderlyingSparkSession}
import org.apache.spark.sql.types.StructType

import zio._
import zio.spark.SparkContext
import zio.spark.parameter._
import zio.spark.rdd.RDD
import zio.spark.sql.DataFrameReader.WithoutSchema
import zio.spark.sql.SparkSession.Conf
import zio.spark.sql.streaming.DataStreamReader

import scala.reflect.runtime.universe.TypeTag

final case class SparkSession(underlyingSparkSession: UnderlyingSparkSession)
    extends ExtraSparkSessionFeature(underlyingSparkSession) {

  val sparkContext: SparkContext = SparkContext(underlyingSparkSession.sparkContext)

  /** Closes the current SparkSession. */
  def close(implicit trace: Trace): Task[Unit] = ZIO.attempt(underlyingSparkSession.close())

  /** Executes a SQL query using Spark. */
  def sql(sqlText: String)(implicit trace: Trace): Task[DataFrame] =
    ZIO.attempt(Dataset(underlyingSparkSession.sql(sqlText)))

  /** Creates a new [[Dataset]] of type T containing zero elements. */
  def emptyDataset[T: Encoder]: Dataset[T] = Dataset(underlyingSparkSession.emptyDataset[T])

  /**
   * Creates a [[Dataset]] from a local Seq of data of a given type.
   * This method requires an encoder (to convert a JVM object of type
   * `T` to and from the internal Spark SQL representation) that is
   * generally created automatically through implicits from a
   * `SparkSession`, or can be created explicitly by calling static
   * methods on [[Encoders]].
   *
   * ==Example==
   *
   * {{{
   *
   *   import spark.implicits._
   *   case class Person(name: String, age: Long)
   *   val data = Seq(Person("Michael", 29), Person("Andy", 30), Person("Justin", 19))
   *   val ds = spark.createDataset(data)
   *
   *   ds.show()
   *   // +-------+---+
   *   // |   name|age|
   *   // +-------+---+
   *   // |Michael| 29|
   *   // |   Andy| 30|
   *   // | Justin| 19|
   *   // +-------+---+
   * }}}
   *
   * @since 2.0.0
   */
  def createDataset[T: Encoder](data: Seq[T]): Task[Dataset[T]] =
    ZIO.attempt(Dataset(underlyingSparkSession.createDataset(data)))

  /**
   * Creates a [[Dataset]] from an RDD of a given type. This method
   * requires an encoder (to convert a JVM object of type `T` to and
   * from the internal Spark SQL representation) that is generally
   * created automatically through implicits from a `SparkSession`, or
   * can be created explicitly by calling static methods on
   * [[Encoders]].
   *
   * @since 2.0.0
   */
  def createDataset[T: Encoder](data: RDD[T]): Task[Dataset[T]] =
    ZIO.attempt(Dataset(underlyingSparkSession.createDataset(data.underlying)))

  /**
   * Creates a `DataFrame` from an RDD of Product (e.g. case classes,
   * tuples).
   *
   * @since 2.0.0
   */
  def createDataFrame[A <: Product: TypeTag](rdd: RDD[A]): Task[DataFrame] =
    ZIO.attempt(Dataset(underlyingSparkSession.createDataFrame(rdd.underlying)))

  /**
   * Creates a `DataFrame` from a local Seq of Product.
   *
   * @since 2.0.0
   */
  def createDataFrame[A <: Product: TypeTag](data: Seq[A]): Task[DataFrame] =
    ZIO.attempt(Dataset(underlyingSparkSession.createDataFrame(data)))

  /**
   * Creates a `DataFrame` from an `RDD` containing [[Row]]s using the
   * given schema. It is important to make sure that the structure of
   * every [[Row]] of the provided RDD matches the provided schema.
   * Otherwise, there will be runtime exception. Example:
   * {{{
   *   import org.apache.spark.sql._
   *   import org.apache.spark.sql.types._
   *   val sparkSession = new org.apache.spark.sql.SparkSession(sc)
   *
   *   val schema =
   *     StructType(
   *       StructField("name", StringType, false) ::
   *       StructField("age", IntegerType, true) :: Nil)
   *
   *   val people =
   *     sc.textFile("examples/src/main/resources/people.txt").map(
   *       _.split(",")).map(p => Row(p(0), p(1).trim.toInt))
   *   val dataFrame = sparkSession.createDataFrame(people, schema)
   *   dataFrame.printSchema
   *   // root
   *   // |-- name: string (nullable = false)
   *   // |-- age: integer (nullable = true)
   *
   *   dataFrame.createOrReplaceTempView("people")
   *   sparkSession.sql("select name from people").collect.foreach(println)
   * }}}
   *
   * @since 2.0.0
   */
  def createDataFrame(rowRDD: RDD[Row], schema: StructType): Task[DataFrame] =
    ZIO.attempt(Dataset(underlyingSparkSession.createDataFrame(rowRDD.underlying, schema)))

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

  /**
   * Creates a [[Dataset]] from a local Seq of data of a given type.
   * This method requires an encoder (to convert a JVM object of type
   * `T` to and from the internal Spark SQL representation) that is
   * generally created automatically through implicits from a
   * `SparkSession`, or can be created explicitly by calling static
   * methods on [[Encoders]].
   *
   * ==Example==
   *
   * {{{
   *
   *   import spark.implicits._
   *   case class Person(name: String, age: Long)
   *   val data = Seq(Person("Michael", 29), Person("Andy", 30), Person("Justin", 19))
   *   val ds = spark.createDataset(data)
   *
   *   ds.show()
   *   // +-------+---+
   *   // |   name|age|
   *   // +-------+---+
   *   // |Michael| 29|
   *   // |   Andy| 30|
   *   // | Justin| 19|
   *   // +-------+---+
   * }}}
   *
   * @since 2.0.0
   */
  def createDataset[T: Encoder](data: Seq[T]): RIO[SparkSession, Dataset[T]] =
    ZIO.service[SparkSession].flatMap(_.createDataset(data))

  /**
   * Creates a [[Dataset]] from an RDD of a given type. This method
   * requires an encoder (to convert a JVM object of type `T` to and
   * from the internal Spark SQL representation) that is generally
   * created automatically through implicits from a `SparkSession`, or
   * can be created explicitly by calling static methods on
   * [[Encoders]].
   *
   * @since 2.0.0
   */
  def createDataset[T: Encoder](data: RDD[T]): RIO[SparkSession, Dataset[T]] =
    ZIO.service[SparkSession].flatMap(_.createDataset(data))

  /**
   * Creates a `DataFrame` from an RDD of Product (e.g. case classes,
   * tuples).
   *
   * @since 2.0.0
   */
  def createDataFrame[A <: Product: TypeTag](rdd: RDD[A]): RIO[SparkSession, DataFrame] =
    ZIO.service[SparkSession].flatMap(_.createDataFrame(rdd))

  /**
   * Creates a `DataFrame` from a local Seq of Product.
   *
   * @since 2.0.0
   */
  def createDataFrame[A <: Product: TypeTag](data: Seq[A]): RIO[SparkSession, DataFrame] =
    ZIO.service[SparkSession].flatMap(_.createDataFrame(data))

  /**
   * Creates a `DataFrame` from an `RDD` containing [[Row]]s using the
   * given schema. It is important to make sure that the structure of
   * every [[Row]] of the provided RDD matches the provided schema.
   * Otherwise, there will be runtime exception. Example:
   * {{{
   *   import org.apache.spark.sql._
   *   import org.apache.spark.sql.types._
   *   val sparkSession = new org.apache.spark.sql.SparkSession(sc)
   *
   *   val schema =
   *     StructType(
   *       StructField("name", StringType, false) ::
   *       StructField("age", IntegerType, true) :: Nil)
   *
   *   val people =
   *     sc.textFile("examples/src/main/resources/people.txt").map(
   *       _.split(",")).map(p => Row(p(0), p(1).trim.toInt))
   *   val dataFrame = sparkSession.createDataFrame(people, schema)
   *   dataFrame.printSchema
   *   // root
   *   // |-- name: string (nullable = false)
   *   // |-- age: integer (nullable = true)
   *
   *   dataFrame.createOrReplaceTempView("people")
   *   sparkSession.sql("select name from people").collect.foreach(println)
   * }}}
   *
   * @since 2.0.0
   */
  def createDataFrame(rowRDD: RDD[Row], schema: StructType): RIO[SparkSession, DataFrame] =
    ZIO.service[SparkSession].flatMap(_.createDataFrame(rowRDD, schema))

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
      ZIO.acquireRelease(getOrCreate)(ss => ss.close.orDie)

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
