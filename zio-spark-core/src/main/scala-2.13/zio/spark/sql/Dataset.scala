/**
 * /!\ Warning /!\
 *
 * This file is generated using zio-spark-codegen, you should not edit
 * this file directly.
 */

package zio.spark.sql

import org.apache.spark.sql.{
  Column,
  DataFrameNaFunctions => UnderlyingDataFrameNaFunctions,
  DataFrameStatFunctions => UnderlyingDataFrameStatFunctions,
  Dataset => UnderlyingDataset,
  Encoder,
  KeyValueGroupedDataset => UnderlyingKeyValueGroupedDataset,
  RelationalGroupedDataset => UnderlyingRelationalGroupedDataset,
  Row,
  Sniffer,
  TypedColumn
}
import org.apache.spark.sql.Observation
import org.apache.spark.sql.execution.ExplainMode
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel

import zio._
import zio.spark.rdd._
import zio.spark.sql.streaming.DataStreamWriter

import scala.jdk.CollectionConverters._
import scala.reflect.runtime.universe.TypeTag

import java.io.IOException

final case class Dataset[T](underlying: UnderlyingDataset[T]) { self =>
  // scalafix:off
  implicit private def lift[U](x: UnderlyingDataset[U]): Dataset[U]                        = Dataset(x)
  implicit private def iteratorConversion[U](iterator: java.util.Iterator[U]): Iterator[U] = iterator.asScala
  implicit private def liftDataFrameNaFunctions(x: UnderlyingDataFrameNaFunctions): DataFrameNaFunctions =
    DataFrameNaFunctions(x)
  implicit private def liftDataFrameStatFunctions(x: UnderlyingDataFrameStatFunctions): DataFrameStatFunctions =
    DataFrameStatFunctions(x)
  implicit private def liftRelationalGroupedDataset[U](
      x: UnderlyingRelationalGroupedDataset
  ): RelationalGroupedDataset = RelationalGroupedDataset(x)
  implicit private def liftKeyValueGroupedDataset[K, V](
      x: UnderlyingKeyValueGroupedDataset[K, V]
  ): KeyValueGroupedDataset[K, V] = KeyValueGroupedDataset(x)
  // scalafix:on

  /** Applies an action to the underlying Dataset. */
  def action[U](f: UnderlyingDataset[T] => U)(implicit trace: Trace): Task[U] = ZIO.attempt(get(f))

  /** Applies a transformation to the underlying Dataset. */
  def transformation[TNew](f: UnderlyingDataset[T] => UnderlyingDataset[TNew]): Dataset[TNew] = Dataset(f(underlying))

  /**
   * Applies a transformation to the underlying Dataset, it is used for
   * transformations that can fail due to an AnalysisException.
   */
  def transformationWithAnalysis[TNew](f: UnderlyingDataset[T] => UnderlyingDataset[TNew]): TryAnalysis[Dataset[TNew]] =
    TryAnalysis(transformation(f))

  /** Applies an action to the underlying Dataset. */
  def get[U](f: UnderlyingDataset[T] => U): U = f(underlying)

  /**
   * Applies an action to the underlying Dataset, it is used for
   * transformations that can fail due to an AnalysisException.
   */
  def getWithAnalysis[U](f: UnderlyingDataset[T] => U): TryAnalysis[U] = TryAnalysis(f(underlying))

  // Handmade functions specific to zio-spark

  def explain(mode: String)(implicit trace: Trace): SIO[Unit] = explain(ExplainMode.fromString(mode))

  def explain(mode: ExplainMode)(implicit trace: Trace): SIO[Unit] =
    for {
      ss   <- ZIO.service[SparkSession]
      plan <- ss.withActive(underlying.queryExecution.explainString(mode))
      _    <- Console.printLine(plan)
    } yield ()

  def firstOption(implicit trace: Trace): Task[Option[T]] = headOption

  def group(f: UnderlyingDataset[T] => UnderlyingRelationalGroupedDataset): RelationalGroupedDataset =
    RelationalGroupedDataset(f(underlying))

  def groupBy(cols: Column*): RelationalGroupedDataset = group(_.groupBy(cols: _*))

  def headOption(implicit trace: Trace): Task[Option[T]] = head(1).map(_.headOption)

  def last(implicit trace: Trace): Task[T] = tail

  def lastOption(implicit trace: Trace): Task[Option[T]] = tailOption

  def printSchema(implicit trace: Trace): IO[IOException, Unit] = printSchema(Int.MaxValue)

  def printSchema(level: Int)(implicit trace: Trace): IO[IOException, Unit] =
    Console.printLine(schema.treeString(level))

  def rdd: RDD[T] = RDD(get(_.rdd))

  def show(numRows: Int)(implicit trace: Trace): IO[IOException, Unit] = show(numRows, truncate = true)

  def show(implicit trace: Trace): IO[IOException, Unit] = show(20)

  def show(truncate: Boolean)(implicit trace: Trace): IO[IOException, Unit] = show(20, truncate)

  def show(numRows: Int, truncate: Boolean)(implicit trace: Trace): IO[IOException, Unit] = {
    val trunc         = if (truncate) 20 else 0
    val stringifiedDf = Sniffer.datasetShowString(underlying, numRows, truncate = trunc)
    Console.printLine(stringifiedDf)
  }

  def summary(statistics: Statistics*)(implicit d: DummyImplicit): DataFrame =
    self.summary(statistics.map(_.toString): _*)

  def tail(implicit trace: Trace): Task[T] = self.tail(1).map(_.head)

  def tailOption(implicit trace: Trace): Task[Option[T]] = self.tail(1).map(_.headOption)

  def takeRight(n: Int)(implicit trace: Trace): Task[Seq[T]] = self.tail(n)

  def transform[U](t: Dataset[T] => Dataset[U]): Dataset[U] = t(self)

  def unpersistBlocking(implicit trace: Trace): UIO[Dataset[T]] =
    ZIO.succeed(transformation(_.unpersist(blocking = true)))

  def where(f: T => Boolean): Dataset[T] = filter(f)

  def write: DataFrameWriter[T] = DataFrameWriter(self)

  def writeStream: DataStreamWriter[T] = DataStreamWriter(self)

  // Generated functions coming from spark

  def columns: Seq[String] = get(_.columns.toSeq)

  def groupByKey[K: Encoder](func: T => K): KeyValueGroupedDataset[K, T] = get(_.groupByKey[K](func))

  // scalastyle:on println
  /**
   * Returns a [[DataFrameNaFunctions]] for working with missing data.
   * {{{
   *   // Dropping rows containing any null values.
   *   ds.na.drop()
   * }}}
   *
   * @group untypedrel
   * @since 1.6.0
   */
  def na: DataFrameNaFunctions = get(_.na)

  def schema: StructType = get(_.schema)

  def stat: DataFrameStatFunctions = get(_.stat)

  // ===============

  def col(colName: String): TryAnalysis[Column] = getWithAnalysis(_.col(colName))

  def colRegex(colName: String): TryAnalysis[Column] = getWithAnalysis(_.colRegex(colName))

  def cube(cols: Column*): TryAnalysis[RelationalGroupedDataset] = getWithAnalysis(_.cube(cols: _*))

  def cube(col1: String, cols: String*): TryAnalysis[RelationalGroupedDataset] = getWithAnalysis(_.cube(col1, cols: _*))

  def rollup(cols: Column*): TryAnalysis[RelationalGroupedDataset] = getWithAnalysis(_.rollup(cols: _*))

  def rollup(col1: String, cols: String*): TryAnalysis[RelationalGroupedDataset] =
    getWithAnalysis(_.rollup(col1, cols: _*))

  // ===============

  def collect(implicit trace: Trace): Task[Seq[T]] = action(_.collect().toSeq)

  def count(implicit trace: Trace): Task[Long] = action(_.count())

  def first(implicit trace: Trace): Task[T] = action(_.first())

  def foreach(f: T => Unit)(implicit trace: Trace): Task[Unit] = action(_.foreach(f))

  def foreachPartition(f: Iterator[T] => Unit)(implicit trace: Trace): Task[Unit] = action(_.foreachPartition(f))

  def head(n: => Int)(implicit trace: Trace): Task[Seq[T]] = action(_.head(n).toSeq)

  def head(implicit trace: Trace): Task[T] = action(_.head())

  def isEmpty(implicit trace: Trace): Task[Boolean] = action(_.isEmpty)

  def reduce(func: (T, T) => T)(implicit trace: Trace): Task[T] = action(_.reduce(func))

  def tail(n: => Int)(implicit trace: Trace): Task[Seq[T]] = action(_.tail(n).toSeq)

  def take(n: => Int)(implicit trace: Trace): Task[Seq[T]] = action(_.take(n).toSeq)

  def toLocalIterator(implicit trace: Trace): Task[Iterator[T]] = action(_.toLocalIterator())

  // ===============

  def cache(implicit trace: Trace): Task[Dataset[T]] = action(_.cache())

  def checkpoint(implicit trace: Trace): Task[Dataset[T]] = action(_.checkpoint())

  def checkpoint(eager: => Boolean)(implicit trace: Trace): Task[Dataset[T]] = action(_.checkpoint(eager))

  def createGlobalTempView(viewName: => String)(implicit trace: Trace): Task[Unit] =
    action(_.createGlobalTempView(viewName))

  def createOrReplaceGlobalTempView(viewName: => String)(implicit trace: Trace): Task[Unit] =
    action(_.createOrReplaceGlobalTempView(viewName))

  def createOrReplaceTempView(viewName: => String)(implicit trace: Trace): Task[Unit] =
    action(_.createOrReplaceTempView(viewName))

  def createTempView(viewName: => String)(implicit trace: Trace): Task[Unit] = action(_.createTempView(viewName))

  def dtypes(implicit trace: Trace): Task[Seq[(String, String)]] = action(_.dtypes.toSeq)

  def inputFiles(implicit trace: Trace): Task[Seq[String]] = action(_.inputFiles.toSeq)

  def isLocal(implicit trace: Trace): Task[Boolean] = action(_.isLocal)

  def isStreaming(implicit trace: Trace): Task[Boolean] = action(_.isStreaming)

  def localCheckpoint(implicit trace: Trace): Task[Dataset[T]] = action(_.localCheckpoint())

  def localCheckpoint(eager: => Boolean)(implicit trace: Trace): Task[Dataset[T]] = action(_.localCheckpoint(eager))

  def persist(implicit trace: Trace): Task[Dataset[T]] = action(_.persist())

  def persist(newLevel: => StorageLevel)(implicit trace: Trace): Task[Dataset[T]] = action(_.persist(newLevel))

  @deprecated("Use createOrReplaceTempView(viewName) instead.", "2.0.0")
  def registerTempTable(tableName: => String)(implicit trace: Trace): Task[Unit] =
    action(_.registerTempTable(tableName))

  def storageLevel(implicit trace: Trace): Task[StorageLevel] = action(_.storageLevel)

  def unpersist(blocking: => Boolean)(implicit trace: Trace): Task[Dataset[T]] = action(_.unpersist(blocking))

  def unpersist(implicit trace: Trace): Task[Dataset[T]] = action(_.unpersist())

  // ===============

  def alias(alias: String): Dataset[T] = transformation(_.alias(alias))

  def alias(alias: Symbol): Dataset[T] = transformation(_.alias(alias))

  def as(alias: String): Dataset[T] = transformation(_.as(alias))

  def as(alias: Symbol): Dataset[T] = transformation(_.as(alias))

  def coalesce(numPartitions: Int): Dataset[T] = transformation(_.coalesce(numPartitions))

  def crossJoin(right: Dataset[_]): DataFrame = transformation(_.crossJoin(right.underlying))

  def distinct: Dataset[T] = transformation(_.distinct())

  def drop(colName: String): DataFrame = transformation(_.drop(colName))

  def drop(colNames: String*): DataFrame = transformation(_.drop(colNames: _*))

  def drop(col: Column): DataFrame = transformation(_.drop(col))

  def drop(col: Column, cols: Column*): DataFrame = transformation(_.drop(col, cols: _*))

  def dropDuplicates: Dataset[T] = transformation(_.dropDuplicates())

  def dropDuplicatesWithinWatermark: Dataset[T] = transformation(_.dropDuplicatesWithinWatermark())

  def except(other: Dataset[T]): Dataset[T] = transformation(_.except(other.underlying))

  def exceptAll(other: Dataset[T]): Dataset[T] = transformation(_.exceptAll(other.underlying))

  @deprecated("use flatMap() or select() with functions.explode() instead", "2.0.0")
  def explode[A <: Product: TypeTag](input: Column*)(f: Row => IterableOnce[A]): DataFrame =
    transformation(_.explode[A](input: _*)(f))

  def filter(func: T => Boolean): Dataset[T] = transformation(_.filter(func))

  def flatMap[U: Encoder](func: T => IterableOnce[U]): Dataset[U] = transformation(_.flatMap[U](func))

  def hint(name: String, parameters: Any*): Dataset[T] = transformation(_.hint(name, parameters: _*))

  def intersect(other: Dataset[T]): Dataset[T] = transformation(_.intersect(other.underlying))

  def intersectAll(other: Dataset[T]): Dataset[T] = transformation(_.intersectAll(other.underlying))

  def join(right: Dataset[_]): DataFrame = transformation(_.join(right.underlying))

  def limit(n: Int): Dataset[T] = transformation(_.limit(n))

  def map[U: Encoder](func: T => U): Dataset[U] = transformation(_.map[U](func))

  def mapPartitions[U: Encoder](func: Iterator[T] => Iterator[U]): Dataset[U] = transformation(_.mapPartitions[U](func))

  def offset(n: Int): Dataset[T] = transformation(_.offset(n))

  def repartition(numPartitions: Int): Dataset[T] = transformation(_.repartition(numPartitions))

  def sample(fraction: Double, seed: Long): Dataset[T] = transformation(_.sample(fraction, seed))

  def sample(fraction: Double): Dataset[T] = transformation(_.sample(fraction))

  def sample(withReplacement: Boolean, fraction: Double, seed: Long): Dataset[T] =
    transformation(_.sample(withReplacement, fraction, seed))

  def sample(withReplacement: Boolean, fraction: Double): Dataset[T] =
    transformation(_.sample(withReplacement, fraction))

  def select[U1](c1: TypedColumn[T, U1]): Dataset[U1] = transformation(_.select[U1](c1))

  def select[U1, U2](c1: TypedColumn[T, U1], c2: TypedColumn[T, U2]): Dataset[(U1, U2)] =
    transformation(_.select[U1, U2](c1, c2))

  def select[U1, U2, U3](
      c1: TypedColumn[T, U1],
      c2: TypedColumn[T, U2],
      c3: TypedColumn[T, U3]
  ): Dataset[(U1, U2, U3)] = transformation(_.select[U1, U2, U3](c1, c2, c3))

  def select[U1, U2, U3, U4](
      c1: TypedColumn[T, U1],
      c2: TypedColumn[T, U2],
      c3: TypedColumn[T, U3],
      c4: TypedColumn[T, U4]
  ): Dataset[(U1, U2, U3, U4)] = transformation(_.select[U1, U2, U3, U4](c1, c2, c3, c4))

  def select[U1, U2, U3, U4, U5](
      c1: TypedColumn[T, U1],
      c2: TypedColumn[T, U2],
      c3: TypedColumn[T, U3],
      c4: TypedColumn[T, U4],
      c5: TypedColumn[T, U5]
  ): Dataset[(U1, U2, U3, U4, U5)] = transformation(_.select[U1, U2, U3, U4, U5](c1, c2, c3, c4, c5))

  def summary(statistics: String*): DataFrame = transformation(_.summary(statistics: _*))

  def to(schema: StructType): DataFrame = transformation(_.to(schema))

  // This is declared with parentheses to prevent the Scala compiler from treating
  // `ds.toDF("1")` as invoking this toDF and then apply on the returned DataFrame.
  def toDF: DataFrame = transformation(_.toDF())

  def toJSON: Dataset[String] = transformation(_.toJSON)

  def union(other: Dataset[T]): Dataset[T] = transformation(_.union(other.underlying))

  def unionAll(other: Dataset[T]): Dataset[T] = transformation(_.unionAll(other.underlying))

  def unionByName(other: Dataset[T]): Dataset[T] = transformation(_.unionByName(other.underlying))

  def withColumnRenamed(existingName: String, newName: String): DataFrame =
    transformation(_.withColumnRenamed(existingName, newName))

  // We only accept an existing column name, not a derived column here as a watermark that is
  // defined on a derived column cannot referenced elsewhere in the plan.
  def withWatermark(eventTime: String, delayThreshold: String): Dataset[T] =
    transformation(_.withWatermark(eventTime, delayThreshold))

  // ===============

  def agg(aggExpr: (String, String), aggExprs: (String, String)*): TryAnalysis[DataFrame] =
    transformationWithAnalysis(_.agg(aggExpr, aggExprs: _*))

  def agg(exprs: Map[String, String]): TryAnalysis[DataFrame] = transformationWithAnalysis(_.agg(exprs))

  def agg(expr: Column, exprs: Column*): TryAnalysis[DataFrame] = transformationWithAnalysis(_.agg(expr, exprs: _*))

  def as[U: Encoder]: TryAnalysis[Dataset[U]] = transformationWithAnalysis(_.as[U])

  def describe(cols: String*): TryAnalysis[DataFrame] = transformationWithAnalysis(_.describe(cols: _*))

  def dropDuplicates(colNames: Seq[String]): TryAnalysis[Dataset[T]] =
    transformationWithAnalysis(_.dropDuplicates(colNames))

  def dropDuplicates(col1: String, cols: String*): TryAnalysis[Dataset[T]] =
    transformationWithAnalysis(_.dropDuplicates(col1, cols: _*))

  def dropDuplicatesWithinWatermark(colNames: Seq[String]): TryAnalysis[Dataset[T]] =
    transformationWithAnalysis(_.dropDuplicatesWithinWatermark(colNames))

  def dropDuplicatesWithinWatermark(col1: String, cols: String*): TryAnalysis[Dataset[T]] =
    transformationWithAnalysis(_.dropDuplicatesWithinWatermark(col1, cols: _*))

  @deprecated("use flatMap() or select() with functions.explode() instead", "2.0.0")
  def explode[A, B: TypeTag](inputColumn: String, outputColumn: String)(
      f: A => IterableOnce[B]
  ): TryAnalysis[DataFrame] = transformationWithAnalysis(_.explode[A, B](inputColumn, outputColumn)(f))

  def filter(condition: Column): TryAnalysis[Dataset[T]] = transformationWithAnalysis(_.filter(condition))

  def filter(conditionExpr: String): TryAnalysis[Dataset[T]] = transformationWithAnalysis(_.filter(conditionExpr))

  def join(right: Dataset[_], usingColumn: String): TryAnalysis[DataFrame] =
    transformationWithAnalysis(_.join(right.underlying, usingColumn))

  def join(right: Dataset[_], usingColumns: Seq[String]): TryAnalysis[DataFrame] =
    transformationWithAnalysis(_.join(right.underlying, usingColumns))

  def join(right: Dataset[_], usingColumn: String, joinType: String): TryAnalysis[DataFrame] =
    transformationWithAnalysis(_.join(right.underlying, usingColumn, joinType))

  def join(right: Dataset[_], usingColumns: Seq[String], joinType: String): TryAnalysis[DataFrame] =
    transformationWithAnalysis(_.join(right.underlying, usingColumns, joinType))

  def join(right: Dataset[_], joinExprs: Column): TryAnalysis[DataFrame] =
    transformationWithAnalysis(_.join(right.underlying, joinExprs))

  def join(right: Dataset[_], joinExprs: Column, joinType: String): TryAnalysis[DataFrame] =
    transformationWithAnalysis(_.join(right.underlying, joinExprs, joinType))

  def joinWith[U](other: Dataset[U], condition: Column, joinType: String): TryAnalysis[Dataset[(T, U)]] =
    transformationWithAnalysis(_.joinWith[U](other.underlying, condition, joinType))

  def joinWith[U](other: Dataset[U], condition: Column): TryAnalysis[Dataset[(T, U)]] =
    transformationWithAnalysis(_.joinWith[U](other.underlying, condition))

  def observe(name: String, expr: Column, exprs: Column*): TryAnalysis[Dataset[T]] =
    transformationWithAnalysis(_.observe(name, expr, exprs: _*))

  def observe(observation: Observation, expr: Column, exprs: Column*): TryAnalysis[Dataset[T]] =
    transformationWithAnalysis(_.observe(observation, expr, exprs: _*))

  def orderBy(sortCol: String, sortCols: String*): TryAnalysis[Dataset[T]] =
    transformationWithAnalysis(_.orderBy(sortCol, sortCols: _*))

  def orderBy(sortExprs: Column*): TryAnalysis[Dataset[T]] = transformationWithAnalysis(_.orderBy(sortExprs: _*))

  def repartition(numPartitions: Int, partitionExprs: Column*): TryAnalysis[Dataset[T]] =
    transformationWithAnalysis(_.repartition(numPartitions, partitionExprs: _*))

  def repartition(partitionExprs: Column*): TryAnalysis[Dataset[T]] =
    transformationWithAnalysis(_.repartition(partitionExprs: _*))

  def repartitionByRange(numPartitions: Int, partitionExprs: Column*): TryAnalysis[Dataset[T]] =
    transformationWithAnalysis(_.repartitionByRange(numPartitions, partitionExprs: _*))

  def repartitionByRange(partitionExprs: Column*): TryAnalysis[Dataset[T]] =
    transformationWithAnalysis(_.repartitionByRange(partitionExprs: _*))

  def select(cols: Column*): TryAnalysis[DataFrame] = transformationWithAnalysis(_.select(cols: _*))

  def select(col: String, cols: String*): TryAnalysis[DataFrame] = transformationWithAnalysis(_.select(col, cols: _*))

  def selectExpr(exprs: String*): TryAnalysis[DataFrame] = transformationWithAnalysis(_.selectExpr(exprs: _*))

  def sort(sortCol: String, sortCols: String*): TryAnalysis[Dataset[T]] =
    transformationWithAnalysis(_.sort(sortCol, sortCols: _*))

  def sort(sortExprs: Column*): TryAnalysis[Dataset[T]] = transformationWithAnalysis(_.sort(sortExprs: _*))

  def sortWithinPartitions(sortCol: String, sortCols: String*): TryAnalysis[Dataset[T]] =
    transformationWithAnalysis(_.sortWithinPartitions(sortCol, sortCols: _*))

  def sortWithinPartitions(sortExprs: Column*): TryAnalysis[Dataset[T]] =
    transformationWithAnalysis(_.sortWithinPartitions(sortExprs: _*))

  def toDF(colNames: String*): TryAnalysis[DataFrame] = transformationWithAnalysis(_.toDF(colNames: _*))

  def unionByName(other: Dataset[T], allowMissingColumns: Boolean): TryAnalysis[Dataset[T]] =
    transformationWithAnalysis(_.unionByName(other.underlying, allowMissingColumns))

  def where(condition: Column): TryAnalysis[Dataset[T]] = transformationWithAnalysis(_.where(condition))

  def where(conditionExpr: String): TryAnalysis[Dataset[T]] = transformationWithAnalysis(_.where(conditionExpr))

  def withColumn(colName: String, col: Column): TryAnalysis[DataFrame] =
    transformationWithAnalysis(_.withColumn(colName, col))

  def withColumns(colsMap: Map[String, Column]): TryAnalysis[DataFrame] =
    transformationWithAnalysis(_.withColumns(colsMap))

  def withColumnsRenamed(colsMap: Map[String, String]): TryAnalysis[DataFrame] =
    transformationWithAnalysis(_.withColumnsRenamed(colsMap))

  def withMetadata(columnName: String, metadata: Metadata): TryAnalysis[DataFrame] =
    transformationWithAnalysis(_.withMetadata(columnName, metadata))

  // ===============

  // Methods that need to be implemented
  //
  // [[org.apache.spark.sql.Dataset.writeTo]]

  // ===============

  // Methods with handmade implementations
  //
  // [[org.apache.spark.sql.Dataset.explain]]
  // [[org.apache.spark.sql.Dataset.groupBy]]
  // [[org.apache.spark.sql.Dataset.printSchema]]
  // [[org.apache.spark.sql.Dataset.show]]
  // [[org.apache.spark.sql.Dataset.transform]]
  // [[org.apache.spark.sql.Dataset.write]]
  // [[org.apache.spark.sql.Dataset.writeStream]]

  // ===============

  // Ignored methods
  //
  // [[org.apache.spark.sql.Dataset.apply]]
  // [[org.apache.spark.sql.Dataset.collectAsList]]
  // [[org.apache.spark.sql.Dataset.filter]]
  // [[org.apache.spark.sql.Dataset.flatMap]]
  // [[org.apache.spark.sql.Dataset.foreach]]
  // [[org.apache.spark.sql.Dataset.foreachPartition]]
  // [[org.apache.spark.sql.Dataset.groupByKey]]
  // [[org.apache.spark.sql.Dataset.javaRDD]]
  // [[org.apache.spark.sql.Dataset.map]]
  // [[org.apache.spark.sql.Dataset.mapPartitions]]
  // [[org.apache.spark.sql.Dataset.reduce]]
  // [[org.apache.spark.sql.Dataset.takeAsList]]
  // [[org.apache.spark.sql.Dataset.toJavaRDD]]
  // [[org.apache.spark.sql.Dataset.toString]]

  // ===============

  def metadataColumn(colName: String): Column = get(_.metadataColumn(colName))
}
