package zio.spark.sql

import org.apache.spark.sql.{
  DataFrameReader => UnderlyingDataFrameReader,
  Dataset => UnderlyingDataset,
  SparkSession => UnderlyingSparkSession
}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.types.StructType

import zio.Trace
import zio.spark.sql.DataFrameReader.SchemaState
import zio.spark.sql.SchemaFromCaseClass.ToStructSchema

import java.util.Properties

final case class DataFrameReader[State <: SchemaState] private[sql] (
    options:             Map[String, String],
    userSpecifiedSchema: Option[StructType]
) {
  import DataFrameReader._

  private def construct(ss: UnderlyingSparkSession): UnderlyingDataFrameReader = {
    val base = ss.read.options(options)

    userSpecifiedSchema match {
      case None         => base
      case Some(schema) => base.schema(schema)
    }
  }

  /**
   * Loads a dataframe from a CSV file.
   *
   * See [[UnderlyingDataFrameReader.csv]] for more information.
   */
  def csv(path: String)(implicit trace: Trace): SIO[DataFrame] = csv(Seq(path): _*)

  /**
   * Loads a dataframe from CSV files.
   *
   * See [[UnderlyingDataFrameReader.csv]] for more information.
   */
  def csv(paths: String*)(implicit trace: Trace): SIO[DataFrame] = loadUsing(_.csv(paths: _*))

  /**
   * Loads a dataframe from Dataset[String].
   *
   * See [[UnderlyingDataFrameReader.csv]] for more information.
   */
  def csv(csvDataset: Dataset[String])(implicit trace: Trace): SIO[DataFrame] = loadUsing(_.csv(csvDataset.underlying))

  /**
   * Loads a dataframe from a JSON file.
   *
   * See [[UnderlyingDataFrameReader.json]] for more information.
   */
  def json(path: String)(implicit trace: Trace): SIO[DataFrame] = json(Seq(path): _*)

  /**
   * Loads a dataframe from JSON files.
   *
   * See [[UnderlyingDataFrameReader.json]] for more information.
   */
  def json(paths: String*)(implicit trace: Trace): SIO[DataFrame] = loadUsing(_.json(paths: _*))

  /**
   * Loads a dataframe from a Dataset[String].
   *
   * See [[UnderlyingDataFrameReader.json]] for more information.
   */
  def json(jsonDataset: Dataset[String])(implicit trace: Trace): SIO[DataFrame] =
    loadUsing(_.json(jsonDataset.underlying))

  /**
   * Loads a dataframe from a PARQUET file.
   *
   * See [[UnderlyingDataFrameReader.parquet]] for more information.
   */
  def parquet(path: String)(implicit trace: Trace): SIO[DataFrame] = parquet(Seq(path): _*)

  /**
   * Loads a dataframe from PARQUET files.
   *
   * See [[UnderlyingDataFrameReader.parquet]] for more information.
   */
  def parquet(paths: String*)(implicit trace: Trace): SIO[DataFrame] = loadUsing(_.parquet(paths: _*))

  /**
   * Loads a dataframe from a ORC file.
   *
   * See [[UnderlyingDataFrameReader.orc]] for more information.
   */
  def orc(path: String)(implicit trace: Trace): SIO[DataFrame] = orc(Seq(path): _*)

  /**
   * Loads a dataframe from ORC files.
   *
   * See [[UnderlyingDataFrameReader.orc]] for more information.
   */
  def orc(paths: String*)(implicit trace: Trace): SIO[DataFrame] = loadUsing(_.orc(paths: _*))

  /**
   * Loads a dataframe from a text file.
   *
   * The underlying schema of the Dataset contains a single string
   * column named "value". The text files must be encoded as UTF-8.
   *
   * See [[UnderlyingDataFrameReader.textFile]] for more information.
   */
  def text(path: String)(implicit trace: Trace, ev: State =:= WithoutSchema): SIO[DataFrame] = text(Seq(path): _*)

  /**
   * Loads a dataframe from text files.
   *
   * The underlying schema of the Dataset contains a single string
   * column named "value". The text files must be encoded as UTF-8.
   *
   * See [[UnderlyingDataFrameReader.textFile]] for more information.
   */
  def text(path: String*)(implicit trace: Trace, ev: State =:= WithoutSchema): SIO[DataFrame] =
    loadUsing(_.text(path: _*))

  /**
   * Loads a dataset[String] from a text file.
   *
   * See [[UnderlyingDataFrameReader.textFile]] for more information.
   */
  def textFile(path: String)(implicit trace: Trace, ev: State =:= WithoutSchema): SIO[Dataset[String]] =
    textFile(Seq(path): _*)

  /**
   * Loads a dataset[String] from text files.
   *
   * See [[UnderlyingDataFrameReader.textFile]] for more information.
   */
  def textFile(path: String*)(implicit trace: Trace, ev: State =:= WithoutSchema): SIO[Dataset[String]] = {
    import zio.spark.sql.TryAnalysis.syntax._
    import zio.spark.sql.implicits._
    import scala3encoders.given

    text(path: _*).map(_.select("value").as[String])
  }

  /**
   * Returns the specified table/view as a `DataFrame`.
   *
   * See [[UnderlyingDataFrameReader.table]] for more information.
   */
  def table(tableName: String)(implicit trace: Trace, ev: State =:= WithoutSchema): SIO[DataFrame] =
    loadUsing(_.table(tableName))

  /**
   * Construct a `DataFrame` representing the database table accessible
   * via JDBC.
   *
   * See [[UnderlyingDataFrameReader.jdbc]] for more information.
   */
  def jdbc(url: String, table: String, properties: Properties)(implicit
      trace: Trace,
      ev: State =:= WithoutSchema
  ): SIO[DataFrame] = loadUsing(_.jdbc(url, table, properties))

  /**
   * Construct a `DataFrame` representing the database table accessible
   * via JDBC.
   *
   * See [[UnderlyingDataFrameReader.jdbc]] for more information.
   */
  def jdbc(url: String, table: String, predicates: Array[String], connectionProperties: Properties)(implicit
      trace: Trace,
      ev: State =:= WithoutSchema
  ): SIO[DataFrame] = loadUsing(_.jdbc(url, table, predicates, connectionProperties))

  /**
   * Construct a `DataFrame` representing the database table accessible
   * via JDBC.
   *
   * See [[UnderlyingDataFrameReader.jdbc]] for more information.
   */
  def jdbc(
      url: String,
      table: String,
      columnName: String,
      lowerBound: Long,
      upperBound: Long,
      numPartitions: Int,
      connectionProperties: Properties
  )(implicit
      trace: Trace,
      ev: State =:= WithoutSchema
  ): SIO[DataFrame] =
    loadUsing(_.jdbc(url, table, columnName, lowerBound, upperBound, numPartitions, connectionProperties))

  /** Loads a dataframe using one of the dataframe loader. */
  private def loadUsing[T](f: UnderlyingDataFrameReader => UnderlyingDataset[T])(implicit
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
   * See [[UnderlyingDataFrameReader.schema]] for more information.
   */
  def schema(schema: StructType): DataFrameReader[WithSchema] = copy(userSpecifiedSchema = Some(schema))

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
   * See [[UnderlyingDataFrameReader.schema]] for more information.
   */
  def schema(schemaString: String): Either[ParseException, DataFrameReader[WithSchema]] =
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
  def schema[T](implicit T: ToStructSchema[T]): DataFrameReader[WithSchema] = schema(T.toSchema)

  /** Adds multiple options to the DataFrameReader. */
  def options(options: Map[String, String]): DataFrameReader[State] = copy(options = this.options ++ options)

  /** Adds any type of option to the DataFrameReader. */
  private def addOption(key: String, value: Any): DataFrameReader[State] = options(Map(key -> value.toString))

  /** Adds an option to the DataFrameReader. */
  def option(key: String, value: String): DataFrameReader[State] = addOption(key, value)

  /** Adds an option to the DataFrameReader. */
  def option(key: String, value: Boolean): DataFrameReader[State] = addOption(key, value)

  /** Adds an option to the DataFrameReader. */
  def option(key: String, value: Int): DataFrameReader[State] = addOption(key, value)

  /** Adds an option to the DataFrameReader. */
  def option(key: String, value: Float): DataFrameReader[State] = addOption(key, value)

  /** Adds an option to the DataFrameReader. */
  def option(key: String, value: Double): DataFrameReader[State] = addOption(key, value)

  /** Adds an option to delimit the column from a csv file. */
  def withDelimiter(delimiter: String): DataFrameReader[State] = option("delimiter", delimiter)

  /** Adds an option to say that the file has a header. */
  def withHeader: DataFrameReader[State] = option("header", value = true)

  /** Adds an option to say that spark should infer the schema. */
  def inferSchema: DataFrameReader[State] = option("inferSchema", value = true)
}

object DataFrameReader {
  sealed trait SchemaState
  sealed trait WithSchema    extends SchemaState
  sealed trait WithoutSchema extends SchemaState
}
