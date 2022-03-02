/**
 * /!\ Warning /!\
 *
 * This file is generated using zio-spark-codegen, you should not edit
 * this file directly.
 */

package zio.spark.internal.codegen

import org.apache.spark.sql.{Column, Dataset => UnderlyingDataset, Encoder, Row, TypedColumn}
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel

import zio.Task
import zio.spark.impure.Impure
import zio.spark.impure.Impure.ImpureBox
import zio.spark.sql.{DataFrame, Dataset, TryAnalysis}

import scala.jdk.CollectionConverters._
import scala.reflect.runtime.universe.TypeTag

@SuppressWarnings(Array("scalafix:DisableSyntax.defaultArgs", "scalafix:DisableSyntax.null"))
abstract class BaseDataset[T](underlyingDataset: ImpureBox[UnderlyingDataset[T]])
    extends Impure[UnderlyingDataset[T]](underlyingDataset) {
  import underlyingDataset._

  // scalafix:off
  implicit private def lift[U](x: UnderlyingDataset[U]): Dataset[U]   = Dataset(x)
  implicit private def escape[U](x: Dataset[U]): UnderlyingDataset[U] = x.underlyingDataset.succeedNow(v => v)
  implicit private def iteratorConversion[U](iterator: java.util.Iterator[U]): Iterator[U] = iterator.asScala
  // scalafix:on

  /** Applies an action to the underlying Dataset. */
  def action[U](f: UnderlyingDataset[T] => U): Task[U] = attemptBlocking(f)

  /** Applies a transformation to the underlying Dataset. */
  def transformation[U](f: UnderlyingDataset[T] => UnderlyingDataset[U]): Dataset[U] =
    succeedNow(f.andThen(x => Dataset(x)))

  /**
   * Applies a transformation to the underlying dataset, it is used for
   * transformations that can fail due to an AnalysisException.
   */
  def transformationWithAnalysis[U](f: UnderlyingDataset[T] => UnderlyingDataset[U]): TryAnalysis[Dataset[U]] =
    TryAnalysis(transformation(f))

  /**
   * Returns an array that contains all of [[Row]]s in this Dataset.
   *
   * Running collect requires moving all the data into the application's
   * driver process, and doing so on a very large dataset can crash the
   * driver process with OutOfMemoryError.
   *
   * For Java API, use [[collectAsList]].
   *
   * @group action
   * @since 1.6.0
   */
  def collect: Task[Seq[T]] = action(_.collect().toSeq)

  /**
   * Returns the number of rows in the Dataset.
   * @group action
   * @since 1.6.0
   */
  def count: Task[Long] = action(_.count())

  /**
   * Returns the first row. Alias for head().
   * @group action
   * @since 1.6.0
   */
  def first: Task[T] = action(_.first())

  /**
   * Applies a function `f` to all rows.
   *
   * @group action
   * @since 1.6.0
   */
  def foreach(f: T => Unit): Task[Unit] = action(_.foreach(f))

  /**
   * Applies a function `f` to each partition of this Dataset.
   *
   * @group action
   * @since 1.6.0
   */
  def foreachPartition(f: Iterator[T] => Unit): Task[Unit] = action(_.foreachPartition(f))

  /**
   * Returns the first `n` rows.
   *
   * @note
   *   this method should only be used if the resulting array is
   *   expected to be small, as all the data is loaded into the driver's
   *   memory.
   *
   * @group action
   * @since 1.6.0
   */
  def head(n: Int): Task[Seq[T]] = action(_.head(n).toSeq)

  /**
   * Returns the first row.
   * @group action
   * @since 1.6.0
   */
  def head: Task[T] = action(_.head())

  /**
   * :: Experimental :: (Scala-specific) Reduces the elements of this
   * Dataset using the specified binary function. The given `func` must
   * be commutative and associative or the result may be
   * non-deterministic.
   *
   * @group action
   * @since 1.6.0
   */
  def reduce(func: (T, T) => T): Task[T] = action(_.reduce(func))

  /**
   * Returns the first `n` rows in the Dataset.
   *
   * Running take requires moving data into the application's driver
   * process, and doing so with a very large `n` can crash the driver
   * process with OutOfMemoryError.
   *
   * @group action
   * @since 1.6.0
   */
  def take(n: Int): Task[Seq[T]] = action(_.take(n).toSeq)

  /**
   * Return an iterator that contains all of [[Row]]s in this Dataset.
   *
   * The iterator will consume as much memory as the largest partition
   * in this Dataset.
   *
   * @note
   *   this results in multiple Spark jobs, and if the input Dataset is
   *   the result of a wide transformation (e.g. join with different
   *   partitioners), to avoid recomputing the input Dataset should be
   *   cached first.
   *
   * @group action
   * @since 2.0.0
   */
  def toLocalIterator: Task[Iterator[T]] = action(_.toLocalIterator())

  // ===============

  /**
   * Persist this Dataset with the default storage level
   * (`MEMORY_AND_DISK`).
   *
   * @group basic
   * @since 1.6.0
   */
  def cache: Task[Dataset[T]] = action(_.cache())

  /**
   * Eagerly checkpoint a Dataset and return the new Dataset.
   * Checkpointing can be used to truncate the logical plan of this
   * Dataset, which is especially useful in iterative algorithms where
   * the plan may grow exponentially. It will be saved to files inside
   * the checkpoint directory set with `SparkContext#setCheckpointDir`.
   *
   * @group basic
   * @since 2.1.0
   */
  def checkpoint: Task[Dataset[T]] = action(_.checkpoint())

  /**
   * Returns a checkpointed version of this Dataset. Checkpointing can
   * be used to truncate the logical plan of this Dataset, which is
   * especially useful in iterative algorithms where the plan may grow
   * exponentially. It will be saved to files inside the checkpoint
   * directory set with `SparkContext#setCheckpointDir`.
   *
   * @group basic
   * @since 2.1.0
   */
  def checkpoint(eager: Boolean): Task[Dataset[T]] = action(_.checkpoint(eager))

  /**
   * Returns all column names as an array.
   *
   * @group basic
   * @since 1.6.0
   */
  def columns: Task[Seq[String]] = action(_.columns.toSeq)

  /**
   * Creates a global temporary view using the given name. The lifetime
   * of this temporary view is tied to this Spark application.
   *
   * Global temporary view is cross-session. Its lifetime is the
   * lifetime of the Spark application,
   * i.e. it will be automatically dropped when the application
   * terminates. It's tied to a system preserved database
   * `_global_temp`, and we must use the qualified name to refer a
   * global temp view, e.g. `SELECT * FROM _global_temp.view1`.
   *
   * @throws AnalysisException
   *   if the view name already exists
   *
   * @group basic
   * @since 2.1.0
   */
  def createGlobalTempView(viewName: String): Task[Unit] = action(_.createGlobalTempView(viewName))

  /**
   * Creates a local temporary view using the given name. The lifetime
   * of this temporary view is tied to the [[SparkSession]] that was
   * used to create this Dataset.
   *
   * @group basic
   * @since 2.0.0
   */
  def createOrReplaceTempView(viewName: String): Task[Unit] = action(_.createOrReplaceTempView(viewName))

  /**
   * Creates a local temporary view using the given name. The lifetime
   * of this temporary view is tied to the [[SparkSession]] that was
   * used to create this Dataset.
   *
   * Local temporary view is session-scoped. Its lifetime is the
   * lifetime of the session that created it, i.e. it will be
   * automatically dropped when the session terminates. It's not tied to
   * any databases, i.e. we can't use `db1.view1` to reference a local
   * temporary view.
   *
   * @throws AnalysisException
   *   if the view name already exists
   *
   * @group basic
   * @since 2.0.0
   */
  def createTempView(viewName: String): Task[Unit] = action(_.createTempView(viewName))

  /**
   * Returns all column names and their data types as an array.
   *
   * @group basic
   * @since 1.6.0
   */
  def dtypes: Task[Seq[(String, String)]] = action(_.dtypes.toSeq)

  /**
   * Returns a best-effort snapshot of the files that compose this
   * Dataset. This method simply asks each constituent BaseRelation for
   * its respective files and takes the union of all results. Depending
   * on the source relations, this may not find all input files.
   * Duplicates are removed.
   *
   * @group basic
   * @since 2.0.0
   */
  def inputFiles: Task[Seq[String]] = action(_.inputFiles.toSeq)

  /**
   * Returns true if the `collect` and `take` methods can be run locally
   * (without any Spark executors).
   *
   * @group basic
   * @since 1.6.0
   */
  def isLocal: Task[Boolean] = action(_.isLocal)

  /**
   * Returns true if this Dataset contains one or more sources that
   * continuously return data as it arrives. A Dataset that reads data
   * from a streaming source must be executed as a `StreamingQuery`
   * using the `start()` method in `DataStreamWriter`. Methods that
   * return a single answer, e.g. `count()` or `collect()`, will throw
   * an [[AnalysisException]] when there is a streaming source present.
   *
   * @group streaming
   * @since 2.0.0
   */
  def isStreaming: Task[Boolean] = action(_.isStreaming)

  /**
   * Persist this Dataset with the default storage level
   * (`MEMORY_AND_DISK`).
   *
   * @group basic
   * @since 1.6.0
   */
  def persist: Task[Dataset[T]] = action(_.persist())

  /**
   * Persist this Dataset with the given storage level.
   * @param newLevel
   *   One of: `MEMORY_ONLY`, `MEMORY_AND_DISK`, `MEMORY_ONLY_SER`,
   *   `MEMORY_AND_DISK_SER`, `DISK_ONLY`, `MEMORY_ONLY_2`,
   *   `MEMORY_AND_DISK_2`, etc.
   *
   * @group basic
   * @since 1.6.0
   */
  def persist(newLevel: StorageLevel): Task[Dataset[T]] = action(_.persist(newLevel))

  /**
   * Registers this Dataset as a temporary table using the given name.
   * The lifetime of this temporary table is tied to the
   * [[SparkSession]] that was used to create this Dataset.
   *
   * @group basic
   * @since 1.6.0
   */
  @deprecated("Use createOrReplaceTempView(viewName) instead.", "2.0.0")
  def registerTempTable(tableName: String): Task[Unit] = action(_.registerTempTable(tableName))

  /**
   * Returns the schema of this Dataset.
   *
   * @group basic
   * @since 1.6.0
   */
  def schema: Task[StructType] = action(_.schema)

  /**
   * Get the Dataset's current storage level, or StorageLevel.NONE if
   * not persisted.
   *
   * @group basic
   * @since 2.1.0
   */
  def storageLevel: Task[StorageLevel] = action(_.storageLevel)

  /**
   * Mark the Dataset as non-persistent, and remove all blocks for it
   * from memory and disk.
   *
   * @param blocking
   *   Whether to block until all blocks are deleted.
   *
   * @group basic
   * @since 1.6.0
   */
  def unpersist(blocking: Boolean): Task[Dataset[T]] = action(_.unpersist(blocking))

  /**
   * Mark the Dataset as non-persistent, and remove all blocks for it
   * from memory and disk.
   *
   * @group basic
   * @since 1.6.0
   */
  def unpersist: Task[Dataset[T]] = action(_.unpersist())

  // ===============

  /**
   * (Scala-specific) Aggregates on the entire Dataset without groups.
   * {{{
   *   // ds.agg(...) is a shorthand for ds.groupBy().agg(...)
   *   ds.agg("age" -> "max", "salary" -> "avg")
   *   ds.groupBy().agg("age" -> "max", "salary" -> "avg")
   * }}}
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def agg(aggExpr: (String, String), aggExprs: (String, String)*): TryAnalysis[DataFrame] =
    transformationWithAnalysis(_.agg(aggExpr, aggExprs: _*))

  /**
   * (Scala-specific) Aggregates on the entire Dataset without groups.
   * {{{
   *   // ds.agg(...) is a shorthand for ds.groupBy().agg(...)
   *   ds.agg(Map("age" -> "max", "salary" -> "avg"))
   *   ds.groupBy().agg(Map("age" -> "max", "salary" -> "avg"))
   * }}}
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def agg(exprs: Map[String, String]): TryAnalysis[DataFrame] = transformationWithAnalysis(_.agg(exprs))

  /**
   * (Java-specific) Aggregates on the entire Dataset without groups.
   * {{{
   *   // ds.agg(...) is a shorthand for ds.groupBy().agg(...)
   *   ds.agg(Map("age" -> "max", "salary" -> "avg"))
   *   ds.groupBy().agg(Map("age" -> "max", "salary" -> "avg"))
   * }}}
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def agg(exprs: java.util.Map[String, String]): TryAnalysis[DataFrame] = transformationWithAnalysis(_.agg(exprs))

  /**
   * Aggregates on the entire Dataset without groups.
   * {{{
   *   // ds.agg(...) is a shorthand for ds.groupBy().agg(...)
   *   ds.agg(max($"age"), avg($"salary"))
   *   ds.groupBy().agg(max($"age"), avg($"salary"))
   * }}}
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def agg(expr: Column, exprs: Column*): TryAnalysis[DataFrame] = transformationWithAnalysis(_.agg(expr, exprs: _*))

  /**
   * Returns a new Dataset with an alias set. Same as `as`.
   *
   * @group typedrel
   * @since 2.0.0
   */
  def alias(alias: String): Dataset[T] = transformation(_.alias(alias))

  /**
   * (Scala-specific) Returns a new Dataset with an alias set. Same as
   * `as`.
   *
   * @group typedrel
   * @since 2.0.0
   */
  def alias(alias: Symbol): Dataset[T] = transformation(_.alias(alias))

  /**
   * :: Experimental :: Returns a new Dataset where each record has been
   * mapped on to the specified type. The method used to map columns
   * depend on the type of `U`:
   *   - When `U` is a class, fields for the class will be mapped to
   *     columns of the same name (case sensitivity is determined by
   *     `spark.sql.caseSensitive`).
   *   - When `U` is a tuple, the columns will be be mapped by ordinal
   *     (i.e. the first column will be assigned to `_1`).
   *   - When `U` is a primitive type (i.e. String, Int, etc), then the
   *     first column of the `DataFrame` will be used.
   *
   * If the schema of the Dataset does not match the desired `U` type,
   * you can use `select` along with `alias` or `as` to rearrange or
   * rename as required.
   *
   * Note that `as[]` only changes the view of the data that is passed
   * into typed operations, such as `map()`, and does not eagerly
   * project away any columns that are not present in the specified
   * class.
   *
   * @group basic
   * @since 1.6.0
   */
  def as[U: Encoder]: TryAnalysis[Dataset[U]] = transformationWithAnalysis(_.as)

  /**
   * Returns a new Dataset with an alias set.
   *
   * @group typedrel
   * @since 1.6.0
   */
  def as(alias: String): Dataset[T] = transformation(_.as(alias))

  /**
   * (Scala-specific) Returns a new Dataset with an alias set.
   *
   * @group typedrel
   * @since 2.0.0
   */
  def as(alias: Symbol): Dataset[T] = transformation(_.as(alias))

  /**
   * Returns a new Dataset that has exactly `numPartitions` partitions.
   * Similar to coalesce defined on an `RDD`, this operation results in
   * a narrow dependency, e.g. if you go from 1000 partitions to 100
   * partitions, there will not be a shuffle, instead each of the 100
   * new partitions will claim 10 of the current partitions. If a larger
   * number of partitions is requested, it will stay at the current
   * number of partitions.
   *
   * However, if you're doing a drastic coalesce, this may result in
   * your computation taking place on fewer nodes than you like (e.g.
   * one node in the case of numPartitions = 1). To avoid this, you can
   * call repartition. This will add a shuffle step, but means the
   * current upstream partitions will be executed in parallel (per
   * whatever the current partitioning is).
   *
   * @group typedrel
   * @since 1.6.0
   */
  def coalesce(numPartitions: Int): Dataset[T] = transformation(_.coalesce(numPartitions))

  /**
   * Explicit cartesian join with another `DataFrame`.
   *
   * @param right
   *   Right side of the join operation.
   *
   * @note
   *   Cartesian joins are very expensive without an extra filter that
   *   can be pushed down.
   *
   * @group untypedrel
   * @since 2.1.0
   */
  def crossJoin(right: Dataset[_]): DataFrame = transformation(_.crossJoin(right))

  /**
   * Computes statistics for numeric and string columns, including
   * count, mean, stddev, min, and max. If no columns are given, this
   * function computes statistics for all numerical or string columns.
   *
   * This function is meant for exploratory data analysis, as we make no
   * guarantee about the backward compatibility of the schema of the
   * resulting Dataset. If you want to programmatically compute summary
   * statistics, use the `agg` function instead.
   *
   * {{{
   *   ds.describe("age", "height").show()
   *
   *   // output:
   *   // summary age   height
   *   // count   10.0  10.0
   *   // mean    53.3  178.05
   *   // stddev  11.6  15.7
   *   // min     18.0  163.0
   *   // max     92.0  192.0
   * }}}
   *
   * @group action
   * @since 1.6.0
   */
  def describe(cols: String*): DataFrame = transformation(_.describe(cols: _*))

  /**
   * Returns a new Dataset that contains only the unique rows from this
   * Dataset. This is an alias for `dropDuplicates`.
   *
   * @note
   *   Equality checking is performed directly on the encoded
   *   representation of the data and thus is not affected by a custom
   *   `equals` function defined on `T`.
   *
   * @group typedrel
   * @since 2.0.0
   */
  def distinct: Dataset[T] = transformation(_.distinct())

  /**
   * Returns a new Dataset with a column dropped. This is a no-op if
   * schema doesn't contain column name.
   *
   * This method can only be used to drop top level columns. the colName
   * string is treated literally without further interpretation.
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def drop(colName: String): DataFrame = transformation(_.drop(colName))

  /**
   * Returns a new Dataset with columns dropped. This is a no-op if
   * schema doesn't contain column name(s).
   *
   * This method can only be used to drop top level columns. the colName
   * string is treated literally without further interpretation.
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def drop(colNames: String*): DataFrame = transformation(_.drop(colNames: _*))

  /**
   * Returns a new Dataset with a column dropped. This version of drop
   * accepts a [[Column]] rather than a name. This is a no-op if the
   * Dataset doesn't have a column with an equivalent expression.
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def drop(col: Column): DataFrame = transformation(_.drop(col))

  /**
   * Returns a new Dataset that contains only the unique rows from this
   * Dataset. This is an alias for `distinct`.
   *
   * @group typedrel
   * @since 2.0.0
   */
  def dropDuplicates: Dataset[T] = transformation(_.dropDuplicates())

  /**
   * (Scala-specific) Returns a new Dataset with duplicate rows removed,
   * considering only the subset of columns.
   *
   * @group typedrel
   * @since 2.0.0
   */
  def dropDuplicates(colNames: Seq[String]): Dataset[T] = transformation(_.dropDuplicates(colNames))

  /**
   * Returns a new Dataset with duplicate rows removed, considering only
   * the subset of columns.
   *
   * @group typedrel
   * @since 2.0.0
   */
  def dropDuplicates(colNames: Array[String]): Dataset[T] = transformation(_.dropDuplicates(colNames))

  /**
   * Returns a new [[Dataset]] with duplicate rows removed, considering
   * only the subset of columns.
   *
   * @group typedrel
   * @since 2.0.0
   */
  def dropDuplicates(col1: String, cols: String*): Dataset[T] = transformation(_.dropDuplicates(col1, cols: _*))

  /**
   * Returns a new Dataset containing rows in this Dataset but not in
   * another Dataset. This is equivalent to `EXCEPT` in SQL.
   *
   * @note
   *   Equality checking is performed directly on the encoded
   *   representation of the data and thus is not affected by a custom
   *   `equals` function defined on `T`.
   *
   * @group typedrel
   * @since 2.0.0
   */
  def except(other: Dataset[T]): Dataset[T] = transformation(_.except(other))

  /**
   * (Scala-specific) Returns a new Dataset where each row has been
   * expanded to zero or more rows by the provided function. This is
   * similar to a `LATERAL VIEW` in HiveQL. The columns of the input row
   * are implicitly joined with each row that is output by the function.
   *
   * Given that this is deprecated, as an alternative, you can explode
   * columns either using `functions.explode()` or `flatMap()`. The
   * following example uses these alternatives to count the number of
   * books that contain a given word:
   *
   * {{{
   *   case class Book(title: String, words: String)
   *   val ds: Dataset[Book]
   *
   *   val allWords = ds.select('title, explode(split('words, " ")).as("word"))
   *
   *   val bookCountPerWord = allWords.groupBy("word").agg(countDistinct("title"))
   * }}}
   *
   * Using `flatMap()` this can similarly be exploded as:
   *
   * {{{
   *   ds.flatMap(_.words.split(" "))
   * }}}
   *
   * @group untypedrel
   * @since 2.0.0
   */
  @deprecated("use flatMap() or select() with functions.explode() instead", "2.0.0")
  def explode[A <: Product: TypeTag](input: Column*)(f: Row => IterableOnce[A]): DataFrame =
    transformation(_.explode(input: _*)(f))

  /**
   * (Scala-specific) Returns a new Dataset where a single column has
   * been expanded to zero or more rows by the provided function. This
   * is similar to a `LATERAL VIEW` in HiveQL. All columns of the input
   * row are implicitly joined with each value that is output by the
   * function.
   *
   * Given that this is deprecated, as an alternative, you can explode
   * columns either using `functions.explode()`:
   *
   * {{{
   *   ds.select(explode(split('words, " ")).as("word"))
   * }}}
   *
   * or `flatMap()`:
   *
   * {{{
   *   ds.flatMap(_.words.split(" "))
   * }}}
   *
   * @group untypedrel
   * @since 2.0.0
   */
  @deprecated("use flatMap() or select() with functions.explode() instead", "2.0.0")
  def explode[A, B: TypeTag](inputColumn: String, outputColumn: String)(f: A => IterableOnce[B]): DataFrame =
    transformation(_.explode(inputColumn, outputColumn)(f))

  /**
   * Filters rows using the given condition.
   * {{{
   *   // The following are equivalent:
   *   peopleDs.filter($"age" > 15)
   *   peopleDs.where($"age" > 15)
   * }}}
   *
   * @group typedrel
   * @since 1.6.0
   */
  def filter(condition: Column): TryAnalysis[Dataset[T]] = transformationWithAnalysis(_.filter(condition))

  /**
   * Filters rows using the given SQL expression.
   * {{{
   *   peopleDs.filter("age > 15")
   * }}}
   *
   * @group typedrel
   * @since 1.6.0
   */
  def filter(conditionExpr: String): TryAnalysis[Dataset[T]] = transformationWithAnalysis(_.filter(conditionExpr))

  /**
   * :: Experimental :: (Scala-specific) Returns a new Dataset that only
   * contains elements where `func` returns `true`.
   *
   * @group typedrel
   * @since 1.6.0
   */
  def filter(func: T => Boolean): Dataset[T] = transformation(_.filter(func))

  /**
   * :: Experimental :: (Scala-specific) Returns a new Dataset by first
   * applying a function to all elements of this Dataset, and then
   * flattening the results.
   *
   * @group typedrel
   * @since 1.6.0
   */
  def flatMap[U: Encoder](func: T => IterableOnce[U]): Dataset[U] = transformation(_.flatMap(func))

  /**
   * Returns a new Dataset containing rows only in both this Dataset and
   * another Dataset. This is equivalent to `INTERSECT` in SQL.
   *
   * @note
   *   Equality checking is performed directly on the encoded
   *   representation of the data and thus is not affected by a custom
   *   `equals` function defined on `T`.
   *
   * @group typedrel
   * @since 1.6.0
   */
  def intersect(other: Dataset[T]): Dataset[T] = transformation(_.intersect(other))

  /**
   * Join with another `DataFrame`.
   *
   * Behaves as an INNER JOIN and requires a subsequent join predicate.
   *
   * @param right
   *   Right side of the join operation.
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def join(right: Dataset[_]): DataFrame = transformation(_.join(right))

  /**
   * Inner equi-join with another `DataFrame` using the given column.
   *
   * Different from other join functions, the join column will only
   * appear once in the output,
   * i.e. similar to SQL's `JOIN USING` syntax.
   *
   * {{{
   *   // Joining df1 and df2 using the column "user_id"
   *   df1.join(df2, "user_id")
   * }}}
   *
   * @param right
   *   Right side of the join operation.
   * @param usingColumn
   *   Name of the column to join on. This column must exist on both
   *   sides.
   *
   * @note
   *   If you perform a self-join using this function without aliasing
   *   the input `DataFrame`s, you will NOT be able to reference any
   *   columns after the join, since there is no way to disambiguate
   *   which side of the join you would like to reference.
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def join(right: Dataset[_], usingColumn: String): DataFrame = transformation(_.join(right, usingColumn))

  /**
   * Inner equi-join with another `DataFrame` using the given columns.
   *
   * Different from other join functions, the join columns will only
   * appear once in the output,
   * i.e. similar to SQL's `JOIN USING` syntax.
   *
   * {{{
   *   // Joining df1 and df2 using the columns "user_id" and "user_name"
   *   df1.join(df2, Seq("user_id", "user_name"))
   * }}}
   *
   * @param right
   *   Right side of the join operation.
   * @param usingColumns
   *   Names of the columns to join on. This columns must exist on both
   *   sides.
   *
   * @note
   *   If you perform a self-join using this function without aliasing
   *   the input `DataFrame`s, you will NOT be able to reference any
   *   columns after the join, since there is no way to disambiguate
   *   which side of the join you would like to reference.
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def join(right: Dataset[_], usingColumns: Seq[String]): DataFrame = transformation(_.join(right, usingColumns))

  /**
   * Equi-join with another `DataFrame` using the given columns. A cross
   * join with a predicate is specified as an inner join. If you would
   * explicitly like to perform a cross join use the `crossJoin` method.
   *
   * Different from other join functions, the join columns will only
   * appear once in the output,
   * i.e. similar to SQL's `JOIN USING` syntax.
   *
   * @param right
   *   Right side of the join operation.
   * @param usingColumns
   *   Names of the columns to join on. This columns must exist on both
   *   sides.
   * @param joinType
   *   Type of join to perform. Default `inner`. Must be one of:
   *   `inner`, `cross`, `outer`, `full`, `full_outer`, `left`,
   *   `left_outer`, `right`, `right_outer`, `left_semi`, `left_anti`.
   *
   * @note
   *   If you perform a self-join using this function without aliasing
   *   the input `DataFrame`s, you will NOT be able to reference any
   *   columns after the join, since there is no way to disambiguate
   *   which side of the join you would like to reference.
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def join(right: Dataset[_], usingColumns: Seq[String], joinType: String): DataFrame =
    transformation(_.join(right, usingColumns, joinType))

  /**
   * Inner join with another `DataFrame`, using the given join
   * expression.
   *
   * {{{
   *   // The following two are equivalent:
   *   df1.join(df2, $"df1Key" === $"df2Key")
   *   df1.join(df2).where($"df1Key" === $"df2Key")
   * }}}
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def join(right: Dataset[_], joinExprs: Column): TryAnalysis[DataFrame] =
    transformationWithAnalysis(_.join(right, joinExprs))

  /**
   * Join with another `DataFrame`, using the given join expression. The
   * following performs a full outer join between `df1` and `df2`.
   *
   * {{{
   *   // Scala:
   *   import org.apache.spark.sql.functions._
   *   df1.join(df2, $"df1Key" === $"df2Key", "outer")
   *
   *   // Java:
   *   import static org.apache.spark.sql.functions.*;
   *   df1.join(df2, col("df1Key").equalTo(col("df2Key")), "outer");
   * }}}
   *
   * @param right
   *   Right side of the join.
   * @param joinExprs
   *   Join expression.
   * @param joinType
   *   Type of join to perform. Default `inner`. Must be one of:
   *   `inner`, `cross`, `outer`, `full`, `full_outer`, `left`,
   *   `left_outer`, `right`, `right_outer`, `left_semi`, `left_anti`.
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def join(right: Dataset[_], joinExprs: Column, joinType: String): TryAnalysis[DataFrame] =
    transformationWithAnalysis(_.join(right, joinExprs, joinType))

  /**
   * :: Experimental :: Joins this Dataset returning a `Tuple2` for each
   * pair where `condition` evaluates to true.
   *
   * This is similar to the relation `join` function with one important
   * difference in the result schema. Since `joinWith` preserves objects
   * present on either side of the join, the result schema is similarly
   * nested into a tuple under the column names `_1` and `_2`.
   *
   * This type of join can be useful both for preserving type-safety
   * with the original object types as well as working with relational
   * data where either side of the join has column names in common.
   *
   * @param other
   *   Right side of the join.
   * @param condition
   *   Join expression.
   * @param joinType
   *   Type of join to perform. Default `inner`. Must be one of:
   *   `inner`, `cross`, `outer`, `full`, `full_outer`, `left`,
   *   `left_outer`, `right`, `right_outer`, `left_semi`, `left_anti`.
   *
   * @group typedrel
   * @since 1.6.0
   */
  def joinWith[U](other: Dataset[U], condition: Column, joinType: String): TryAnalysis[Dataset[(T, U)]] =
    transformationWithAnalysis(_.joinWith(other, condition, joinType))

  /**
   * :: Experimental :: Using inner equi-join to join this Dataset
   * returning a `Tuple2` for each pair where `condition` evaluates to
   * true.
   *
   * @param other
   *   Right side of the join.
   * @param condition
   *   Join expression.
   *
   * @group typedrel
   * @since 1.6.0
   */
  def joinWith[U](other: Dataset[U], condition: Column): TryAnalysis[Dataset[(T, U)]] =
    transformationWithAnalysis(_.joinWith(other, condition))

  /**
   * Returns a new Dataset by taking the first `n` rows. The difference
   * between this function and `head` is that `head` is an action and
   * returns an array (by triggering query execution) while `limit`
   * returns a new Dataset.
   *
   * @group typedrel
   * @since 2.0.0
   */
  def limit(n: Int): Dataset[T] = transformation(_.limit(n))

  /**
   * :: Experimental :: (Scala-specific) Returns a new Dataset that
   * contains the result of applying `func` to each element.
   *
   * @group typedrel
   * @since 1.6.0
   */
  def map[U: Encoder](func: T => U): Dataset[U] = transformation(_.map(func))

  /**
   * :: Experimental :: (Scala-specific) Returns a new Dataset that
   * contains the result of applying `func` to each partition.
   *
   * @group typedrel
   * @since 1.6.0
   */
  def mapPartitions[U: Encoder](func: Iterator[T] => Iterator[U]): Dataset[U] = transformation(_.mapPartitions(func))

  /**
   * Returns a new Dataset sorted by the given expressions. This is an
   * alias of the `sort` function.
   *
   * @group typedrel
   * @since 2.0.0
   */
  def orderBy(sortCol: String, sortCols: String*): Dataset[T] = transformation(_.orderBy(sortCol, sortCols: _*))

  /**
   * Returns a new Dataset sorted by the given expressions. This is an
   * alias of the `sort` function.
   *
   * @group typedrel
   * @since 2.0.0
   */
  def orderBy(sortExprs: Column*): TryAnalysis[Dataset[T]] = transformationWithAnalysis(_.orderBy(sortExprs: _*))

  /**
   * Returns a new Dataset that has exactly `numPartitions` partitions.
   *
   * @group typedrel
   * @since 1.6.0
   */
  def repartition(numPartitions: Int): Dataset[T] = transformation(_.repartition(numPartitions))

  /**
   * Returns a new Dataset partitioned by the given partitioning
   * expressions into `numPartitions`. The resulting Dataset is hash
   * partitioned.
   *
   * This is the same operation as "DISTRIBUTE BY" in SQL (Hive QL).
   *
   * @group typedrel
   * @since 2.0.0
   */
  def repartition(numPartitions: Int, partitionExprs: Column*): TryAnalysis[Dataset[T]] =
    transformationWithAnalysis(_.repartition(numPartitions, partitionExprs: _*))

  /**
   * Returns a new Dataset partitioned by the given partitioning
   * expressions, using `spark.sql.shuffle.partitions` as number of
   * partitions. The resulting Dataset is hash partitioned.
   *
   * This is the same operation as "DISTRIBUTE BY" in SQL (Hive QL).
   *
   * @group typedrel
   * @since 2.0.0
   */
  def repartition(partitionExprs: Column*): TryAnalysis[Dataset[T]] =
    transformationWithAnalysis(_.repartition(partitionExprs: _*))

  /**
   * Returns a new [[Dataset]] by sampling a fraction of rows, using a
   * user-supplied seed.
   *
   * @param withReplacement
   *   Sample with replacement or not.
   * @param fraction
   *   Fraction of rows to generate.
   * @param seed
   *   Seed for sampling.
   *
   * @note
   *   This is NOT guaranteed to provide exactly the fraction of the
   *   count of the given [[Dataset]].
   *
   * @group typedrel
   * @since 1.6.0
   */
  def sample(withReplacement: Boolean, fraction: Double, seed: Long): Dataset[T] =
    transformation(_.sample(withReplacement, fraction, seed))

  /**
   * Returns a new [[Dataset]] by sampling a fraction of rows, using a
   * random seed.
   *
   * @param withReplacement
   *   Sample with replacement or not.
   * @param fraction
   *   Fraction of rows to generate.
   *
   * @note
   *   This is NOT guaranteed to provide exactly the fraction of the
   *   total count of the given [[Dataset]].
   *
   * @group typedrel
   * @since 1.6.0
   */
  def sample(withReplacement: Boolean, fraction: Double): Dataset[T] =
    transformation(_.sample(withReplacement, fraction))

  /**
   * Selects a set of column based expressions.
   * {{{
   *   ds.select($"colA", $"colB" + 1)
   * }}}
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def select(cols: Column*): DataFrame = transformation(_.select(cols: _*))

  /**
   * Selects a set of columns. This is a variant of `select` that can
   * only select existing columns using column names (i.e. cannot
   * construct expressions).
   *
   * {{{
   *   // The following two are equivalent:
   *   ds.select("colA", "colB")
   *   ds.select($"colA", $"colB")
   * }}}
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def select(col: String, cols: String*): DataFrame = transformation(_.select(col, cols: _*))

  /**
   * :: Experimental :: Returns a new Dataset by computing the given
   * [[Column]] expression for each element.
   *
   * {{{
   *   val ds = Seq(1, 2, 3).toDS()
   *   val newDS = ds.select(expr("value + 1").as[Int])
   * }}}
   *
   * @group typedrel
   * @since 1.6.0
   */
  def select[U1](c1: TypedColumn[T, U1]): Dataset[U1] = transformation(_.select(c1))

  /**
   * :: Experimental :: Returns a new Dataset by computing the given
   * [[Column]] expressions for each element.
   *
   * @group typedrel
   * @since 1.6.0
   */
  def select[U1, U2](c1: TypedColumn[T, U1], c2: TypedColumn[T, U2]): Dataset[(U1, U2)] =
    transformation(_.select(c1, c2))

  /**
   * :: Experimental :: Returns a new Dataset by computing the given
   * [[Column]] expressions for each element.
   *
   * @group typedrel
   * @since 1.6.0
   */
  def select[U1, U2, U3](
      c1: TypedColumn[T, U1],
      c2: TypedColumn[T, U2],
      c3: TypedColumn[T, U3]
  ): Dataset[(U1, U2, U3)] = transformation(_.select(c1, c2, c3))

  /**
   * :: Experimental :: Returns a new Dataset by computing the given
   * [[Column]] expressions for each element.
   *
   * @group typedrel
   * @since 1.6.0
   */
  def select[U1, U2, U3, U4](
      c1: TypedColumn[T, U1],
      c2: TypedColumn[T, U2],
      c3: TypedColumn[T, U3],
      c4: TypedColumn[T, U4]
  ): Dataset[(U1, U2, U3, U4)] = transformation(_.select(c1, c2, c3, c4))

  /**
   * :: Experimental :: Returns a new Dataset by computing the given
   * [[Column]] expressions for each element.
   *
   * @group typedrel
   * @since 1.6.0
   */
  def select[U1, U2, U3, U4, U5](
      c1: TypedColumn[T, U1],
      c2: TypedColumn[T, U2],
      c3: TypedColumn[T, U3],
      c4: TypedColumn[T, U4],
      c5: TypedColumn[T, U5]
  ): Dataset[(U1, U2, U3, U4, U5)] = transformation(_.select(c1, c2, c3, c4, c5))

  /**
   * Selects a set of SQL expressions. This is a variant of `select`
   * that accepts SQL expressions.
   *
   * {{{
   *   // The following are equivalent:
   *   ds.selectExpr("colA", "colB as newName", "abs(colC)")
   *   ds.select(expr("colA"), expr("colB as newName"), expr("abs(colC)"))
   * }}}
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def selectExpr(exprs: String*): TryAnalysis[DataFrame] = transformationWithAnalysis(_.selectExpr(exprs: _*))

  /**
   * Returns a new Dataset sorted by the specified column, all in
   * ascending order.
   * {{{
   *   // The following 3 are equivalent
   *   ds.sort("sortcol")
   *   ds.sort($"sortcol")
   *   ds.sort($"sortcol".asc)
   * }}}
   *
   * @group typedrel
   * @since 2.0.0
   */
  def sort(sortCol: String, sortCols: String*): Dataset[T] = transformation(_.sort(sortCol, sortCols: _*))

  /**
   * Returns a new Dataset sorted by the given expressions. For example:
   * {{{
   *   ds.sort($"col1", $"col2".desc)
   * }}}
   *
   * @group typedrel
   * @since 2.0.0
   */
  def sort(sortExprs: Column*): TryAnalysis[Dataset[T]] = transformationWithAnalysis(_.sort(sortExprs: _*))

  /**
   * Returns a new Dataset with each partition sorted by the given
   * expressions.
   *
   * This is the same operation as "SORT BY" in SQL (Hive QL).
   *
   * @group typedrel
   * @since 2.0.0
   */
  def sortWithinPartitions(sortCol: String, sortCols: String*): Dataset[T] =
    transformation(_.sortWithinPartitions(sortCol, sortCols: _*))

  /**
   * Returns a new Dataset with each partition sorted by the given
   * expressions.
   *
   * This is the same operation as "SORT BY" in SQL (Hive QL).
   *
   * @group typedrel
   * @since 2.0.0
   */
  def sortWithinPartitions(sortExprs: Column*): TryAnalysis[Dataset[T]] =
    transformationWithAnalysis(_.sortWithinPartitions(sortExprs: _*))

  /**
   * Converts this strongly typed collection of data to generic
   * Dataframe. In contrast to the strongly typed objects that Dataset
   * operations work on, a Dataframe returns generic [[Row]] objects
   * that allow fields to be accessed by ordinal or name.
   *
   * @group basic
   * @since 1.6.0
   */
  // This is declared with parentheses to prevent the Scala compiler from treating
  // `ds.toDF("1")` as invoking this toDF and then apply on the returned DataFrame.
  def toDF: DataFrame = transformation(_.toDF())

  /**
   * Converts this strongly typed collection of data to generic
   * `DataFrame` with columns renamed. This can be quite convenient in
   * conversion from an RDD of tuples into a `DataFrame` with meaningful
   * names. For example:
   * {{{
   *   val rdd: RDD[(Int, String)] = ...
   *   rdd.toDF()  // this implicit conversion creates a DataFrame with column name `_1` and `_2`
   *   rdd.toDF("id", "name")  // this creates a DataFrame with column name "id" and "name"
   * }}}
   *
   * @group basic
   * @since 2.0.0
   */
  def toDF(colNames: String*): DataFrame = transformation(_.toDF(colNames: _*))

  /**
   * Returns the content of the Dataset as a Dataset of JSON strings.
   * @since 2.0.0
   */
  def toJSON: Dataset[String] = transformation(_.toJSON)

  /**
   * Returns a new Dataset containing union of rows in this Dataset and
   * another Dataset. This is equivalent to `UNION ALL` in SQL.
   *
   * To do a SQL-style set union (that does deduplication of elements),
   * use this function followed by a [[distinct]].
   *
   * @group typedrel
   * @since 2.0.0
   */
  def union(other: Dataset[T]): Dataset[T] = transformation(_.union(other))

  /**
   * Returns a new Dataset containing union of rows in this Dataset and
   * another Dataset. This is equivalent to `UNION ALL` in SQL.
   *
   * To do a SQL-style set union (that does deduplication of elements),
   * use this function followed by a [[distinct]].
   *
   * @group typedrel
   * @since 2.0.0
   */
  @deprecated("use union()", "2.0.0")
  def unionAll(other: Dataset[T]): Dataset[T] = transformation(_.unionAll(other))

  /**
   * Filters rows using the given condition. This is an alias for
   * `filter`.
   * {{{
   *   // The following are equivalent:
   *   peopleDs.filter($"age" > 15)
   *   peopleDs.where($"age" > 15)
   * }}}
   *
   * @group typedrel
   * @since 1.6.0
   */
  def where(condition: Column): TryAnalysis[Dataset[T]] = transformationWithAnalysis(_.where(condition))

  /**
   * Filters rows using the given SQL expression.
   * {{{
   *   peopleDs.where("age > 15")
   * }}}
   *
   * @group typedrel
   * @since 1.6.0
   */
  def where(conditionExpr: String): TryAnalysis[Dataset[T]] = transformationWithAnalysis(_.where(conditionExpr))

  /**
   * Returns a new Dataset by adding a column or replacing the existing
   * column that has the same name.
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def withColumn(colName: String, col: Column): TryAnalysis[DataFrame] =
    transformationWithAnalysis(_.withColumn(colName, col))

  /**
   * Returns a new Dataset with a column renamed. This is a no-op if
   * schema doesn't contain existingName.
   *
   * @group untypedrel
   * @since 2.0.0
   */
  def withColumnRenamed(existingName: String, newName: String): DataFrame =
    transformation(_.withColumnRenamed(existingName, newName))

  /**
   * :: Experimental :: Defines an event time watermark for this
   * [[Dataset]]. A watermark tracks a point in time before which we
   * assume no more late data is going to arrive.
   *
   * Spark will use this watermark for several purposes:
   *   - To know when a given time window aggregation can be finalized
   *     and thus can be emitted when using output modes that do not
   *     allow updates.
   *   - To minimize the amount of state that we need to keep for
   *     on-going aggregations.
   *
   * The current watermark is computed by looking at the
   * `MAX(eventTime)` seen across all of the partitions in the query
   * minus a user specified `delayThreshold`. Due to the cost of
   * coordinating this value across partitions, the actual watermark
   * used is only guaranteed to be at least `delayThreshold` behind the
   * actual event time. In some cases we may still process records that
   * arrive more than `delayThreshold` late.
   *
   * @param eventTime
   *   the name of the column that contains the event time of the row.
   * @param delayThreshold
   *   the minimum delay to wait to data to arrive late, relative to the
   *   latest record that has been processed in the form of an interval
   *   (e.g. "1 minute" or "5 hours"). NOTE: This should not be
   *   negative.
   *
   * @group streaming
   * @since 2.1.0
   */
  def withWatermark(eventTime: String, delayThreshold: String): Dataset[T] =
    transformation(_.withWatermark(eventTime, delayThreshold))

  // ===============

  /**
   * Methods that need to be implemented
   *
   * [[org.apache.spark.sql.Dataset.cube]]
   * [[org.apache.spark.sql.Dataset.explain]]
   * [[org.apache.spark.sql.Dataset.groupByKey]]
   * [[org.apache.spark.sql.Dataset.na]]
   * [[org.apache.spark.sql.Dataset.printSchema]]
   * [[org.apache.spark.sql.Dataset.randomSplit]]
   * [[org.apache.spark.sql.Dataset.rollup]]
   * [[org.apache.spark.sql.Dataset.stat]]
   * [[org.apache.spark.sql.Dataset.writeStream]]
   */

  // ===============

  /**
   * Methods with handmade implementations
   *
   * [[org.apache.spark.sql.Dataset.groupBy]]
   * [[org.apache.spark.sql.Dataset.show]]
   * [[org.apache.spark.sql.Dataset.transform]]
   * [[org.apache.spark.sql.Dataset.write]]
   */

  // ===============

  /**
   * Ignored methods
   *
   * [[org.apache.spark.sql.Dataset.apply]]
   * [[org.apache.spark.sql.Dataset.col]]
   * [[org.apache.spark.sql.Dataset.collectAsList]]
   * [[org.apache.spark.sql.Dataset.filter]]
   * [[org.apache.spark.sql.Dataset.flatMap]]
   * [[org.apache.spark.sql.Dataset.foreach]]
   * [[org.apache.spark.sql.Dataset.foreachPartition]]
   * [[org.apache.spark.sql.Dataset.javaRDD]]
   * [[org.apache.spark.sql.Dataset.map]]
   * [[org.apache.spark.sql.Dataset.mapPartitions]]
   * [[org.apache.spark.sql.Dataset.randomSplitAsList]]
   * [[org.apache.spark.sql.Dataset.reduce]]
   * [[org.apache.spark.sql.Dataset.takeAsList]]
   * [[org.apache.spark.sql.Dataset.toJavaRDD]]
   * [[org.apache.spark.sql.Dataset.toString]]
   */
}
