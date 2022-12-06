package zio.spark.sql.streaming

import org.apache.spark.sql.{Dataset => UnderlyingDataset, SparkSession => UnderlyingSparkSession}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.streaming.{DataStreamReader => UnderlyingDataStreamReader}
import org.apache.spark.sql.types.StructType

import zio.Trace
import zio.spark.sql.{fromSpark, DataFrame, Dataset, SIO}
import zio.spark.sql.SchemaFromCaseClass.ToStructSchema

final case class DataStreamReader private[sql] (
    options:             Map[String, String],
    userSpecifiedSchema: Option[StructType]
) {

  private def construct(ss: UnderlyingSparkSession): UnderlyingDataStreamReader = {
    val base = ss.readStream.options(options)

    userSpecifiedSchema match {
      case None         => base
      case Some(schema) => base.schema(schema)
    }
  }

  /**
   * Loads a dataframe from a folder containing a stream of CSV files.
   *
   * See [[UnderlyingDataStreamReader.csv]] for more information.
   */
  def csv(path: String)(implicit trace: Trace): SIO[DataFrame] = loadUsing(_.csv(path))

  /**
   * Loads a dataframe from a folder containing a stream of JSON files.
   *
   * See [[UnderlyingDataStreamReader.json]] for more information.
   */
  def json(path: String)(implicit trace: Trace): SIO[DataFrame] = loadUsing(_.json(path))

  /**
   * Loads a dataframe from a folder containing a stream of PARQUET
   * files.
   *
   * See [[UnderlyingDataStreamReader.parquet]] for more information.
   */
  def parquet(path: String)(implicit trace: Trace): SIO[DataFrame] = loadUsing(_.parquet(path))

  /**
   * Loads a dataframe from a folder containing a stream of ORC files.
   *
   * See [[UnderlyingDataStreamReader.orc]] for more information.
   */
  def orc(path: String)(implicit trace: Trace): SIO[DataFrame] = loadUsing(_.orc(path))

  /**
   * Loads a dataframe from a folder containing a stream of TXT files.
   *
   * The underlying schema of the Dataset contains a single string
   * column named "value". The text files must be encoded as UTF-8.
   *
   * See [[UnderlyingDataStreamReader.textFile]] for more information.
   */
  def text(path: String)(implicit trace: Trace): SIO[DataFrame] = loadUsing(_.text(path))

  /**
   * Loads a dataset[String] from a folder containing a stream of TXT
   * files.
   *
   * See [[UnderlyingDataStreamReader.textFile]] for more information.
   */
  def textFile(path: String)(implicit trace: Trace): SIO[Dataset[String]] = {
    import zio.spark.sql.TryAnalysis.syntax._
    import zio.spark.sql.implicits._
    import scala3encoders.given

    text(path).map(_.select("value").as[String])
  }

  def socket(host: String, port: Int)(implicit trace: Trace): SIO[DataFrame] =
    option("host", host).option("port", port).loadUsing(_.format("socket").load())

  /** Loads a dataframe using one of the dataframe loader. */
  private def loadUsing[T](f: UnderlyingDataStreamReader => UnderlyingDataset[T])(implicit
      trace: Trace
  ): SIO[Dataset[T]] = fromSpark(ss => Dataset(f(construct(ss))))

  /**
   * Replace the data schema of the DataFrameReader.
   *
   * We advice you to always use a schema even if some data sources can
   * infer the schema. It allows you to increase your job speed, it
   * ensures that the schema is the expected one and it is useful as
   * documentation.
   *
   * See [[UnderlyingDataStreamReader.schema]] for more information.
   */
  def schema(schema: StructType): DataStreamReader = copy(userSpecifiedSchema = Some(schema))

  /**
   * Replace the data schema of the DataFrameReader.
   *
   * We advice you to always use a schema even if some data sources can
   * infer the schema. It allows you to increase your job speed, it
   * ensures that the schema is the expected one and it is useful as
   * documentation.
   *
   * E.g.:
   * {{{
   *   schema("a INT, b STRING, c DOUBLE").csv("test.csv")
   * }}}
   *
   * See [[UnderlyingDataStreamReader.schema]] for more information.
   */
  def schema(schemaString: String): Either[ParseException, DataStreamReader] =
    try Right(schema(StructType.fromDDL(schemaString)))
    catch { case e: ParseException => Left(e) }

  /**
   * ZIO-Spark specifics function to generate the schema from a case
   * class.
   *
   * E.g.:
   * {{{
   *   import zio.spark._
   *
   *   case class Person(name: String, age: Int)
   *   val ds: Dataset[Person] = SparkSession.read.schema[Person].csv("./path.csv").as[Person].getOrThrow
   * }}}
   */
  def schema[T](implicit T: ToStructSchema[T]): DataStreamReader = schema(T.toSchema)

  /** Adds multiple options to the DataFrameReader. */
  def options(options: Map[String, String]): DataStreamReader = copy(options = this.options ++ options)

  /** Adds any type of option to the DataFrameReader. */
  private def addOption(key: String, value: Any): DataStreamReader = options(Map(key -> value.toString))

  /** Adds an option to the DataFrameReader. */
  def option(key: String, value: String): DataStreamReader = addOption(key, value)

  /** Adds an option to the DataFrameReader. */
  def option(key: String, value: Boolean): DataStreamReader = addOption(key, value)

  /** Adds an option to the DataFrameReader. */
  def option(key: String, value: Int): DataStreamReader = addOption(key, value)

  /** Adds an option to the DataFrameReader. */
  def option(key: String, value: Float): DataStreamReader = addOption(key, value)

  /** Adds an option to the DataFrameReader. */
  def option(key: String, value: Double): DataStreamReader = addOption(key, value)
}
