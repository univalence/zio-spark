package zio.spark.internal.codegen

import scala.reflect._

import org.apache.spark.sql.{Dataset => UnderlyingDataset, Column, Encoder, Row, TypedColumn}
import org.apache.spark.storage.StorageLevel
import scala.collection.immutable.Map

import zio.Task
import zio.spark.impure.Impure
import zio.spark.impure.Impure.ImpureBox
import zio.spark.sql.Dataset
import zio.spark.rdd.RDD


abstract class BaseDataset[T](underlyingDataset: ImpureBox[UnderlyingDataset[T]]) extends Impure[UnderlyingDataset[T]](underlyingDataset) {
  import underlyingDataset._

  private implicit def arrayToSeq1[U](x: Dataset[Array[U]])(implicit enc: Encoder[Seq[U]]): Dataset[Seq[U]] = x.map(_.toSeq)
  private implicit def arrayToSeq2[U](x: UnderlyingDataset[Array[U]])(implicit enc: Encoder[Seq[U]]): UnderlyingDataset[Seq[U]] = x.map(_.toSeq)
  private implicit def lift[U](x:UnderlyingDataset[U]):Dataset[U] = Dataset(x)
  private implicit def escape[U](x:Dataset[U]):UnderlyingDataset[U] = x.underlyingDataset.succeedNow(v => v)
  
  private implicit def iteratorConversion[T](iterator: java.util.Iterator[T]):Iterator[T] = scala.collection.JavaConverters.asScalaIteratorConverter(iterator).asScala
  
  /** Applies an action to the underlying Dataset. */
  def action[U](f: UnderlyingDataset[T] => U): Task[U] = attemptBlocking(f)

  /** Applies a transformation to the underlying Dataset. */
  def transformation[U](f: UnderlyingDataset[T] => UnderlyingDataset[U]): Dataset[U] = succeedNow(f.andThen(x => Dataset(x)))

  def collect: Task[Seq[T]] = action(_.collect())
  def count: Task[Long] = action(_.count())
  def first: Task[T] = action(_.first())
  def foreach(f: T => Unit): Task[Unit] = action(_.foreach(f))
  def foreachPartition(f: Iterator[T] => Unit): Task[Unit] = action(_.foreachPartition(f))
  def isEmpty: Task[Boolean] = action(_.isEmpty)
  def reduce(func: (T, T) => T): Task[T] = action(_.reduce(func))
  def take(n: Int): Task[Seq[T]] = action(_.take(n))
  def toLocalIterator: Task[Iterator[T]] = action(_.toLocalIterator())
  
  //===============
  
  def cache: Task[Dataset[T]] = action(_.cache())
  def checkpoint(eager: Boolean): Task[Dataset[T]] = action(_.checkpoint(eager))
  def checkpoint: Task[Dataset[T]] = action(_.checkpoint())
  def localCheckpoint(eager: Boolean): Task[Dataset[T]] = action(_.localCheckpoint(eager))
  def localCheckpoint: Task[Dataset[T]] = action(_.localCheckpoint())
  def persist(newLevel: StorageLevel): Task[Dataset[T]] = action(_.persist(newLevel))
  def persist: Task[Dataset[T]] = action(_.persist())
  def unpersist: Task[Dataset[T]] = action(_.unpersist())
  def unpersist(blocking: Boolean): Task[Dataset[T]] = action(_.unpersist(blocking))
  
  //===============
  
  def agg(expr: Column, exprs: Column*): Dataset[Row] = transformation(_.agg(expr, exprs: _*))
  def agg(exprs: Map[String, String]): Dataset[Row] = transformation(_.agg(exprs))
  def agg(aggExpr: (String, String), aggExprs: (String, String)*): Dataset[Row] = transformation(_.agg(aggExpr, aggExprs: _*))
  def alias(alias: Symbol): Dataset[T] = transformation(_.alias(alias))
  def alias(alias: String): Dataset[T] = transformation(_.alias(alias))
  def as(alias: Symbol): Dataset[T] = transformation(_.as(alias))
  def as(alias: String): Dataset[T] = transformation(_.as(alias))
  def as[U](implicit evidence$2: Encoder[U]): Dataset[U] = transformation(_.as)
  def coalesce(numPartitions: Int): Dataset[T] = transformation(_.coalesce(numPartitions))
  def crossJoin(right: Dataset[_]): Dataset[Row] = transformation(_.crossJoin(right))
  def describe(cols: String*): Dataset[Row] = transformation(_.describe(cols: _*))
  def distinct: Dataset[T] = transformation(_.distinct())
  def drop(col: Column): Dataset[Row] = transformation(_.drop(col))
  def drop(colNames: String*): Dataset[Row] = transformation(_.drop(colNames: _*))
  def drop(colName: String): Dataset[Row] = transformation(_.drop(colName))
  def dropDuplicates(col1: String, cols: String*): Dataset[T] = transformation(_.dropDuplicates(col1, cols: _*))
  def dropDuplicates(colNames: Seq[String]): Dataset[T] = transformation(_.dropDuplicates(colNames))
  def dropDuplicates: Dataset[T] = transformation(_.dropDuplicates())
  def except(other: Dataset[T]): Dataset[T] = transformation(_.except(other))
  def exceptAll(other: Dataset[T]): Dataset[T] = transformation(_.exceptAll(other))
  def filter(func: T => Boolean): Dataset[T] = transformation(_.filter(func))
  def filter(conditionExpr: String): Dataset[T] = transformation(_.filter(conditionExpr))
  def filter(condition: Column): Dataset[T] = transformation(_.filter(condition))
  def flatMap[U](func: T => TraversableOnce[U])(implicit evidence$8: Encoder[U]): Dataset[U] = transformation(_.flatMap(func))
  def hint(name: String, parameters: Any*): Dataset[T] = transformation(_.hint(name, parameters: _*))
  def intersect(other: Dataset[T]): Dataset[T] = transformation(_.intersect(other))
  def intersectAll(other: Dataset[T]): Dataset[T] = transformation(_.intersectAll(other))
  def join(right: Dataset[_], joinExprs: Column, joinType: String): Dataset[Row] = transformation(_.join(right, joinExprs, joinType))
  def join(right: Dataset[_], joinExprs: Column): Dataset[Row] = transformation(_.join(right, joinExprs))
  def join(right: Dataset[_], usingColumns: Seq[String], joinType: String): Dataset[Row] = transformation(_.join(right, usingColumns, joinType))
  def join(right: Dataset[_], usingColumns: Seq[String]): Dataset[Row] = transformation(_.join(right, usingColumns))
  def join(right: Dataset[_], usingColumn: String): Dataset[Row] = transformation(_.join(right, usingColumn))
  def join(right: Dataset[_]): Dataset[Row] = transformation(_.join(right))
  def joinWith[U](other: Dataset[U], condition: Column): Dataset[(T, U)] = transformation(_.joinWith(other, condition))
  def joinWith[U](other: Dataset[U], condition: Column, joinType: String): Dataset[(T, U)] = transformation(_.joinWith(other, condition, joinType))
  def limit(n: Int): Dataset[T] = transformation(_.limit(n))
  def map[U](func: T => U)(implicit evidence$6: Encoder[U]): Dataset[U] = transformation(_.map(func))
  def mapPartitions[U](func: Iterator[T] => Iterator[U])(implicit evidence$7: Encoder[U]): Dataset[U] = transformation(_.mapPartitions(func))
  def observe(name: String, expr: Column, exprs: Column*): Dataset[T] = transformation(_.observe(name, expr, exprs: _*))
  def orderBy(sortExprs: Column*): Dataset[T] = transformation(_.orderBy(sortExprs: _*))
  def orderBy(sortCol: String, sortCols: String*): Dataset[T] = transformation(_.orderBy(sortCol, sortCols: _*))
  def repartition(partitionExprs: Column*): Dataset[T] = transformation(_.repartition(partitionExprs: _*))
  def repartition(numPartitions: Int, partitionExprs: Column*): Dataset[T] = transformation(_.repartition(numPartitions, partitionExprs: _*))
  def repartition(numPartitions: Int): Dataset[T] = transformation(_.repartition(numPartitions))
  def repartitionByRange(partitionExprs: Column*): Dataset[T] = transformation(_.repartitionByRange(partitionExprs: _*))
  def repartitionByRange(numPartitions: Int, partitionExprs: Column*): Dataset[T] = transformation(_.repartitionByRange(numPartitions, partitionExprs: _*))
  def sample(withReplacement: Boolean, fraction: Double): Dataset[T] = transformation(_.sample(withReplacement, fraction))
  def sample(withReplacement: Boolean, fraction: Double, seed: Long): Dataset[T] = transformation(_.sample(withReplacement, fraction, seed))
  def sample(fraction: Double): Dataset[T] = transformation(_.sample(fraction))
  def sample(fraction: Double, seed: Long): Dataset[T] = transformation(_.sample(fraction, seed))
  def select[U1, U2, U3, U4, U5](c1: TypedColumn[T, U1], c2: TypedColumn[T, U2], c3: TypedColumn[T, U3], c4: TypedColumn[T, U4], c5: TypedColumn[T, U5]): Dataset[(U1, U2, U3, U4, U5)] = transformation(_.select(c1, c2, c3, c4, c5))
  def select[U1, U2, U3, U4](c1: TypedColumn[T, U1], c2: TypedColumn[T, U2], c3: TypedColumn[T, U3], c4: TypedColumn[T, U4]): Dataset[(U1, U2, U3, U4)] = transformation(_.select(c1, c2, c3, c4))
  def select[U1, U2, U3](c1: TypedColumn[T, U1], c2: TypedColumn[T, U2], c3: TypedColumn[T, U3]): Dataset[(U1, U2, U3)] = transformation(_.select(c1, c2, c3))
  def select[U1, U2](c1: TypedColumn[T, U1], c2: TypedColumn[T, U2]): Dataset[(U1, U2)] = transformation(_.select(c1, c2))
  def select[U1](c1: TypedColumn[T, U1]): Dataset[U1] = transformation(_.select(c1))
  def select(col: String, cols: String*): Dataset[Row] = transformation(_.select(col, cols: _*))
  def select(cols: Column*): Dataset[Row] = transformation(_.select(cols: _*))
  def selectExpr(exprs: String*): Dataset[Row] = transformation(_.selectExpr(exprs: _*))
  def sort(sortExprs: Column*): Dataset[T] = transformation(_.sort(sortExprs: _*))
  def sort(sortCol: String, sortCols: String*): Dataset[T] = transformation(_.sort(sortCol, sortCols: _*))
  def sortWithinPartitions(sortExprs: Column*): Dataset[T] = transformation(_.sortWithinPartitions(sortExprs: _*))
  def sortWithinPartitions(sortCol: String, sortCols: String*): Dataset[T] = transformation(_.sortWithinPartitions(sortCol, sortCols: _*))
  def summary(statistics: String*): Dataset[Row] = transformation(_.summary(statistics: _*))
  def toDF(colNames: String*): Dataset[Row] = transformation(_.toDF(colNames: _*))
  def toDF: Dataset[Row] = transformation(_.toDF())
  def toJSON: Dataset[String] = transformation(_.toJSON)
  def union(other: Dataset[T]): Dataset[T] = transformation(_.union(other))
  def unionAll(other: Dataset[T]): Dataset[T] = transformation(_.unionAll(other))
  def unionByName(other: Dataset[T], allowMissingColumns: Boolean): Dataset[T] = transformation(_.unionByName(other, allowMissingColumns))
  def unionByName(other: Dataset[T]): Dataset[T] = transformation(_.unionByName(other))
  def where(conditionExpr: String): Dataset[T] = transformation(_.where(conditionExpr))
  def where(condition: Column): Dataset[T] = transformation(_.where(condition))
  def withColumn(colName: String, col: Column): Dataset[Row] = transformation(_.withColumn(colName, col))
  def withColumnRenamed(existingName: String, newName: String): Dataset[Row] = transformation(_.withColumnRenamed(existingName, newName))
  def withWatermark(eventTime: String, delayThreshold: String): Dataset[T] = transformation(_.withWatermark(eventTime, delayThreshold))
  
  //===============
  
  /**
   * Methods to implement
   *
   * [[org.apache.spark.sql.Dataset.explode]]
   * [[org.apache.spark.sql.Dataset.randomSplit]]
   * [[org.apache.spark.sql.Dataset.toJavaRDD]]
   * [[org.apache.spark.sql.Dataset.transform]]
   */
  
  //===============
  
  /**
   * Ignored method
   *
   * [[org.apache.spark.sql.Dataset.apply]]
   * [[org.apache.spark.sql.Dataset.col]]
   * [[org.apache.spark.sql.Dataset.colRegex]]
   * [[org.apache.spark.sql.Dataset.collectAsList]]
   * [[org.apache.spark.sql.Dataset.columns]]
   * [[org.apache.spark.sql.Dataset.createGlobalTempView]]
   * [[org.apache.spark.sql.Dataset.createOrReplaceGlobalTempView]]
   * [[org.apache.spark.sql.Dataset.createOrReplaceTempView]]
   * [[org.apache.spark.sql.Dataset.createTempView]]
   * [[org.apache.spark.sql.Dataset.cube]]
   * [[org.apache.spark.sql.Dataset.dtypes]]
   * [[org.apache.spark.sql.Dataset.encoder]]
   * [[org.apache.spark.sql.Dataset.explain]]
   * [[org.apache.spark.sql.Dataset.filter]]
   * [[org.apache.spark.sql.Dataset.flatMap]]
   * [[org.apache.spark.sql.Dataset.foreach]]
   * [[org.apache.spark.sql.Dataset.foreachPartition]]
   * [[org.apache.spark.sql.Dataset.groupBy]]
   * [[org.apache.spark.sql.Dataset.groupByKey]]
   * [[org.apache.spark.sql.Dataset.head]]
   * [[org.apache.spark.sql.Dataset.inputFiles]]
   * [[org.apache.spark.sql.Dataset.isLocal]]
   * [[org.apache.spark.sql.Dataset.isStreaming]]
   * [[org.apache.spark.sql.Dataset.javaRDD]]
   * [[org.apache.spark.sql.Dataset.map]]
   * [[org.apache.spark.sql.Dataset.mapPartitions]]
   * [[org.apache.spark.sql.Dataset.na]]
   * [[org.apache.spark.sql.Dataset.printSchema]]
   * [[org.apache.spark.sql.Dataset.queryExecution]]
   * [[org.apache.spark.sql.Dataset.randomSplitAsList]]
   * [[org.apache.spark.sql.Dataset.rdd]]
   * [[org.apache.spark.sql.Dataset.reduce]]
   * [[org.apache.spark.sql.Dataset.registerTempTable]]
   * [[org.apache.spark.sql.Dataset.rollup]]
   * [[org.apache.spark.sql.Dataset.sameSemantics]]
   * [[org.apache.spark.sql.Dataset.schema]]
   * [[org.apache.spark.sql.Dataset.semanticHash]]
   * [[org.apache.spark.sql.Dataset.show]]
   * [[org.apache.spark.sql.Dataset.sparkSession]]
   * [[org.apache.spark.sql.Dataset.sqlContext]]
   * [[org.apache.spark.sql.Dataset.stat]]
   * [[org.apache.spark.sql.Dataset.storageLevel]]
   * [[org.apache.spark.sql.Dataset.tail]]
   * [[org.apache.spark.sql.Dataset.takeAsList]]
   * [[org.apache.spark.sql.Dataset.toString]]
   * [[org.apache.spark.sql.Dataset.write]]
   * [[org.apache.spark.sql.Dataset.writeStream]]
   * [[org.apache.spark.sql.Dataset.writeTo]]
   */
}
