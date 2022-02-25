package zio.spark.internal.codegen


import org.apache.spark.sql.{Dataset => UnderlyingDataset, Column, Encoder, Row, TypedColumn}
import org.apache.spark.storage.StorageLevel

import zio.Task
import zio.spark.impure.Impure
import zio.spark.impure.Impure.ImpureBox
import zio.spark.sql.Dataset


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

  /**
     * Returns an array that contains all rows in this Dataset.
     *
     * Running collect requires moving all the data into the application's driver process, and
     * doing so on a very large dataset can crash the driver process with OutOfMemoryError.
     *
     * For Java API, use [[collectAsList]].
     *
     * @group action
     * @since 1.6.0
     */
  def collect: Task[Seq[T]] = action(_.collect())
  
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
     * Returns true if the `Dataset` is empty.
     *
     * @group basic
     * @since 2.4.0
     */
  def isEmpty: Task[Boolean] = action(_.isEmpty)
  
  /**
     * (Scala-specific)
     * Reduces the elements of this Dataset using the specified binary function. The given `func`
     * must be commutative and associative or the result may be non-deterministic.
     *
     * @group action
     * @since 1.6.0
     */
  def reduce(func: (T, T) => T): Task[T] = action(_.reduce(func))
  
  /**
     * Returns the first `n` rows in the Dataset.
     *
     * Running take requires moving data into the application's driver process, and doing so with
     * a very large `n` can crash the driver process with OutOfMemoryError.
     *
     * @group action
     * @since 1.6.0
     */
  def take(n: Int): Task[Seq[T]] = action(_.take(n))
  
  /**
     * Returns an iterator that contains all rows in this Dataset.
     *
     * The iterator will consume as much memory as the largest partition in this Dataset.
     *
     * @note this results in multiple Spark jobs, and if the input Dataset is the result
     * of a wide transformation (e.g. join with different partitioners), to avoid
     * recomputing the input Dataset should be cached first.
     *
     * @group action
     * @since 2.0.0
     */
  def toLocalIterator: Task[Iterator[T]] = action(_.toLocalIterator())
  
  //===============
  
  /**
     * Persist this Dataset with the default storage level (`MEMORY_AND_DISK`).
     *
     * @group basic
     * @since 1.6.0
     */
  def cache: Task[Dataset[T]] = action(_.cache())
  
  /**
     * Eagerly checkpoint a Dataset and return the new Dataset. Checkpointing can be used to truncate
     * the logical plan of this Dataset, which is especially useful in iterative algorithms where the
     * plan may grow exponentially. It will be saved to files inside the checkpoint
     * directory set with `SparkContext#setCheckpointDir`.
     *
     * @group basic
     * @since 2.1.0
     */
  def checkpoint: Task[Dataset[T]] = action(_.checkpoint())
  
  /**
     * Returns a checkpointed version of this Dataset. Checkpointing can be used to truncate the
     * logical plan of this Dataset, which is especially useful in iterative algorithms where the
     * plan may grow exponentially. It will be saved to files inside the checkpoint
     * directory set with `SparkContext#setCheckpointDir`.
     *
     * @group basic
     * @since 2.1.0
     */
  def checkpoint(eager: Boolean): Task[Dataset[T]] = action(_.checkpoint(eager))
  
  /**
     * Eagerly locally checkpoints a Dataset and return the new Dataset. Checkpointing can be
     * used to truncate the logical plan of this Dataset, which is especially useful in iterative
     * algorithms where the plan may grow exponentially. Local checkpoints are written to executor
     * storage and despite potentially faster they are unreliable and may compromise job completion.
     *
     * @group basic
     * @since 2.3.0
     */
  def localCheckpoint: Task[Dataset[T]] = action(_.localCheckpoint())
  
  /**
     * Locally checkpoints a Dataset and return the new Dataset. Checkpointing can be used to truncate
     * the logical plan of this Dataset, which is especially useful in iterative algorithms where the
     * plan may grow exponentially. Local checkpoints are written to executor storage and despite
     * potentially faster they are unreliable and may compromise job completion.
     *
     * @group basic
     * @since 2.3.0
     */
  def localCheckpoint(eager: Boolean): Task[Dataset[T]] = action(_.localCheckpoint(eager))
  
  /**
     * Persist this Dataset with the default storage level (`MEMORY_AND_DISK`).
     *
     * @group basic
     * @since 1.6.0
     */
  def persist: Task[Dataset[T]] = action(_.persist())
  
  /**
     * Persist this Dataset with the given storage level.
     * @param newLevel One of: `MEMORY_ONLY`, `MEMORY_AND_DISK`, `MEMORY_ONLY_SER`,
     *                 `MEMORY_AND_DISK_SER`, `DISK_ONLY`, `MEMORY_ONLY_2`,
     *                 `MEMORY_AND_DISK_2`, etc.
     *
     * @group basic
     * @since 1.6.0
     */
  def persist(newLevel: StorageLevel): Task[Dataset[T]] = action(_.persist(newLevel))
  
  /**
     * Mark the Dataset as non-persistent, and remove all blocks for it from memory and disk.
     * This will not un-persist any cached data that is built upon this Dataset.
     *
     * @param blocking Whether to block until all blocks are deleted.
     *
     * @group basic
     * @since 1.6.0
     */
  def unpersist(blocking: Boolean): Task[Dataset[T]] = action(_.unpersist(blocking))
  
  /**
     * Mark the Dataset as non-persistent, and remove all blocks for it from memory and disk.
     * This will not un-persist any cached data that is built upon this Dataset.
     *
     * @group basic
     * @since 1.6.0
     */
  def unpersist: Task[Dataset[T]] = action(_.unpersist())
  
  //===============
  
  /**
     * Returns a new Dataset with an alias set. Same as `as`.
     *
     * @group typedrel
     * @since 2.0.0
     */
  def alias(alias: String): Dataset[T] = transformation(_.alias(alias))
  
  /**
     * (Scala-specific) Returns a new Dataset with an alias set. Same as `as`.
     *
     * @group typedrel
     * @since 2.0.0
     */
  def alias(alias: Symbol): Dataset[T] = transformation(_.alias(alias))
  
  /**
     * Returns a new Dataset where each record has been mapped on to the specified type. The
     * method used to map columns depend on the type of `U`:
     * <ul>
     *   <li>When `U` is a class, fields for the class will be mapped to columns of the same name
     *   (case sensitivity is determined by `spark.sql.caseSensitive`).</li>
     *   <li>When `U` is a tuple, the columns will be mapped by ordinal (i.e. the first column will
     *   be assigned to `_1`).</li>
     *   <li>When `U` is a primitive type (i.e. String, Int, etc), then the first column of the
     *   `DataFrame` will be used.</li>
     * </ul>
     *
     * If the schema of the Dataset does not match the desired `U` type, you can use `select`
     * along with `alias` or `as` to rearrange or rename as required.
     *
     * Note that `as[]` only changes the view of the data that is passed into typed operations,
     * such as `map()`, and does not eagerly project away any columns that are not present in
     * the specified class.
     *
     * @group basic
     * @since 1.6.0
     */
  def as[U: Encoder]: Dataset[U] = transformation(_.as)
  
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
     * Returns a new Dataset that has exactly `numPartitions` partitions, when the fewer partitions
     * are requested. If a larger number of partitions is requested, it will stay at the current
     * number of partitions. Similar to coalesce defined on an `RDD`, this operation results in
     * a narrow dependency, e.g. if you go from 1000 partitions to 100 partitions, there will not
     * be a shuffle, instead each of the 100 new partitions will claim 10 of the current partitions.
     *
     * However, if you're doing a drastic coalesce, e.g. to numPartitions = 1,
     * this may result in your computation taking place on fewer nodes than
     * you like (e.g. one node in the case of numPartitions = 1). To avoid this,
     * you can call repartition. This will add a shuffle step, but means the
     * current upstream partitions will be executed in parallel (per whatever
     * the current partitioning is).
     *
     * @group typedrel
     * @since 1.6.0
     */
  def coalesce(numPartitions: Int): Dataset[T] = transformation(_.coalesce(numPartitions))
  
  /**
     * Returns a new Dataset that contains only the unique rows from this Dataset.
     * This is an alias for `dropDuplicates`.
     *
     * Note that for a streaming [[Dataset]], this method returns distinct rows only once
     * regardless of the output mode, which the behavior may not be same with `DISTINCT` in SQL
     * against streaming [[Dataset]].
     *
     * @note Equality checking is performed directly on the encoded representation of the data
     * and thus is not affected by a custom `equals` function defined on `T`.
     *
     * @group typedrel
     * @since 2.0.0
     */
  def distinct: Dataset[T] = transformation(_.distinct())
  
  /**
     * Returns a new Dataset that contains only the unique rows from this Dataset.
     * This is an alias for `distinct`.
     *
     * For a static batch [[Dataset]], it just drops duplicate rows. For a streaming [[Dataset]], it
     * will keep all data across triggers as intermediate state to drop duplicates rows. You can use
     * [[withWatermark]] to limit how late the duplicate data can be and system will accordingly limit
     * the state. In addition, too late data older than watermark will be dropped to avoid any
     * possibility of duplicates.
     *
     * @group typedrel
     * @since 2.0.0
     */
  def dropDuplicates: Dataset[T] = transformation(_.dropDuplicates())
  
  /**
     * (Scala-specific) Returns a new Dataset with duplicate rows removed, considering only
     * the subset of columns.
     *
     * For a static batch [[Dataset]], it just drops duplicate rows. For a streaming [[Dataset]], it
     * will keep all data across triggers as intermediate state to drop duplicates rows. You can use
     * [[withWatermark]] to limit how late the duplicate data can be and system will accordingly limit
     * the state. In addition, too late data older than watermark will be dropped to avoid any
     * possibility of duplicates.
     *
     * @group typedrel
     * @since 2.0.0
     */
  def dropDuplicates(colNames: Seq[String]): Dataset[T] = transformation(_.dropDuplicates(colNames))
  
  /**
     * Returns a new Dataset with duplicate rows removed, considering only
     * the subset of columns.
     *
     * For a static batch [[Dataset]], it just drops duplicate rows. For a streaming [[Dataset]], it
     * will keep all data across triggers as intermediate state to drop duplicates rows. You can use
     * [[withWatermark]] to limit how late the duplicate data can be and system will accordingly limit
     * the state. In addition, too late data older than watermark will be dropped to avoid any
     * possibility of duplicates.
     *
     * @group typedrel
     * @since 2.0.0
     */
  def dropDuplicates(colNames: Array[String]): Dataset[T] = transformation(_.dropDuplicates(colNames))
  
  /**
     * Returns a new [[Dataset]] with duplicate rows removed, considering only
     * the subset of columns.
     *
     * For a static batch [[Dataset]], it just drops duplicate rows. For a streaming [[Dataset]], it
     * will keep all data across triggers as intermediate state to drop duplicates rows. You can use
     * [[withWatermark]] to limit how late the duplicate data can be and system will accordingly limit
     * the state. In addition, too late data older than watermark will be dropped to avoid any
     * possibility of duplicates.
     *
     * @group typedrel
     * @since 2.0.0
     */
  def dropDuplicates(col1: String, cols: String*): Dataset[T] = transformation(_.dropDuplicates(col1, cols: _*))
  
  /**
     * Returns a new Dataset containing rows in this Dataset but not in another Dataset.
     * This is equivalent to `EXCEPT DISTINCT` in SQL.
     *
     * @note Equality checking is performed directly on the encoded representation of the data
     * and thus is not affected by a custom `equals` function defined on `T`.
     *
     * @group typedrel
     * @since 2.0.0
     */
  def except(other: Dataset[T]): Dataset[T] = transformation(_.except(other))
  
  /**
     * Returns a new Dataset containing rows in this Dataset but not in another Dataset while
     * preserving the duplicates.
     * This is equivalent to `EXCEPT ALL` in SQL.
     *
     * @note Equality checking is performed directly on the encoded representation of the data
     * and thus is not affected by a custom `equals` function defined on `T`. Also as standard in
     * SQL, this function resolves columns by position (not by name).
     *
     * @group typedrel
     * @since 2.4.0
     */
  def exceptAll(other: Dataset[T]): Dataset[T] = transformation(_.exceptAll(other))
  
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
  def filter(condition: Column): Dataset[T] = transformation(_.filter(condition))
  
  /**
     * Filters rows using the given SQL expression.
     * {{{
     *   peopleDs.filter("age > 15")
     * }}}
     *
     * @group typedrel
     * @since 1.6.0
     */
  def filter(conditionExpr: String): Dataset[T] = transformation(_.filter(conditionExpr))
  
  /**
     * (Scala-specific)
     * Returns a new Dataset that only contains elements where `func` returns `true`.
     *
     * @group typedrel
     * @since 1.6.0
     */
  def filter(func: T => Boolean): Dataset[T] = transformation(_.filter(func))
  
  /**
     * (Scala-specific)
     * Returns a new Dataset by first applying a function to all elements of this Dataset,
     * and then flattening the results.
     *
     * @group typedrel
     * @since 1.6.0
     */
  def flatMap[U: Encoder](func: T => TraversableOnce[U]): Dataset[U] = transformation(_.flatMap(func))
  
  /**
     * Specifies some hint on the current Dataset. As an example, the following code specifies
     * that one of the plan can be broadcasted:
     *
     * {{{
     *   df1.join(df2.hint("broadcast"))
     * }}}
     *
     * @group basic
     * @since 2.2.0
     */
  def hint(name: String, parameters: Any*): Dataset[T] = transformation(_.hint(name, parameters: _*))
  
  /**
     * Returns a new Dataset containing rows only in both this Dataset and another Dataset.
     * This is equivalent to `INTERSECT` in SQL.
     *
     * @note Equality checking is performed directly on the encoded representation of the data
     * and thus is not affected by a custom `equals` function defined on `T`.
     *
     * @group typedrel
     * @since 1.6.0
     */
  def intersect(other: Dataset[T]): Dataset[T] = transformation(_.intersect(other))
  
  /**
     * Returns a new Dataset containing rows only in both this Dataset and another Dataset while
     * preserving the duplicates.
     * This is equivalent to `INTERSECT ALL` in SQL.
     *
     * @note Equality checking is performed directly on the encoded representation of the data
     * and thus is not affected by a custom `equals` function defined on `T`. Also as standard
     * in SQL, this function resolves columns by position (not by name).
     *
     * @group typedrel
     * @since 2.4.0
     */
  def intersectAll(other: Dataset[T]): Dataset[T] = transformation(_.intersectAll(other))
  
  /**
     * Joins this Dataset returning a `Tuple2` for each pair where `condition` evaluates to
     * true.
     *
     * This is similar to the relation `join` function with one important difference in the
     * result schema. Since `joinWith` preserves objects present on either side of the join, the
     * result schema is similarly nested into a tuple under the column names `_1` and `_2`.
     *
     * This type of join can be useful both for preserving type-safety with the original object
     * types as well as working with relational data where either side of the join has column
     * names in common.
     *
     * @param other Right side of the join.
     * @param condition Join expression.
     * @param joinType Type of join to perform. Default `inner`. Must be one of:
     *                 `inner`, `cross`, `outer`, `full`, `fullouter`,`full_outer`, `left`,
     *                 `leftouter`, `left_outer`, `right`, `rightouter`, `right_outer`.
     *
     * @group typedrel
     * @since 1.6.0
     */
  def joinWith[U](other: Dataset[U], condition: Column, joinType: String): Dataset[(T, U)] = transformation(_.joinWith(other, condition, joinType))
  
  /**
     * Using inner equi-join to join this Dataset returning a `Tuple2` for each pair
     * where `condition` evaluates to true.
     *
     * @param other Right side of the join.
     * @param condition Join expression.
     *
     * @group typedrel
     * @since 1.6.0
     */
  def joinWith[U](other: Dataset[U], condition: Column): Dataset[(T, U)] = transformation(_.joinWith(other, condition))
  
  /**
     * Returns a new Dataset by taking the first `n` rows. The difference between this function
     * and `head` is that `head` is an action and returns an array (by triggering query execution)
     * while `limit` returns a new Dataset.
     *
     * @group typedrel
     * @since 2.0.0
     */
  def limit(n: Int): Dataset[T] = transformation(_.limit(n))
  
  /**
     * (Scala-specific)
     * Returns a new Dataset that contains the result of applying `func` to each element.
     *
     * @group typedrel
     * @since 1.6.0
     */
  def map[U: Encoder](func: T => U): Dataset[U] = transformation(_.map(func))
  
  /**
     * (Scala-specific)
     * Returns a new Dataset that contains the result of applying `func` to each partition.
     *
     * @group typedrel
     * @since 1.6.0
     */
  def mapPartitions[U: Encoder](func: Iterator[T] => Iterator[U]): Dataset[U] = transformation(_.mapPartitions(func))
  
  /**
    * Define (named) metrics to observe on the Dataset. This method returns an 'observed' Dataset
    * that returns the same result as the input, with the following guarantees:
    * <ul>
    *   <li>It will compute the defined aggregates (metrics) on all the data that is flowing through
    *   the Dataset at that point.</li>
    *   <li>It will report the value of the defined aggregate columns as soon as we reach a completion
    *   point. A completion point is either the end of a query (batch mode) or the end of a streaming
    *   epoch. The value of the aggregates only reflects the data processed since the previous
    *   completion point.</li>
    * </ul>
    * Please note that continuous execution is currently not supported.
    *
    * The metrics columns must either contain a literal (e.g. lit(42)), or should contain one or
    * more aggregate functions (e.g. sum(a) or sum(a + b) + avg(c) - lit(1)). Expressions that
    * contain references to the input Dataset's columns must always be wrapped in an aggregate
    * function.
    *
    * A user can observe these metrics by either adding
    * [[org.apache.spark.sql.streaming.StreamingQueryListener]] or a
    * [[org.apache.spark.sql.util.QueryExecutionListener]] to the spark session.
    *
    * {{{
    *   // Monitor the metrics using a listener.
    *   spark.streams.addListener(new StreamingQueryListener() {
    *     override def onQueryProgress(event: QueryProgressEvent): Unit = {
    *       event.progress.observedMetrics.asScala.get("my_event").foreach { row =>
    *         // Trigger if the number of errors exceeds 5 percent
    *         val num_rows = row.getAs[Long]("rc")
    *         val num_error_rows = row.getAs[Long]("erc")
    *         val ratio = num_error_rows.toDouble / num_rows
    *         if (ratio > 0.05) {
    *           // Trigger alert
    *         }
    *       }
    *     }
    *     def onQueryStarted(event: QueryStartedEvent): Unit = {}
    *     def onQueryTerminated(event: QueryTerminatedEvent): Unit = {}
    *   })
    *   // Observe row count (rc) and error row count (erc) in the streaming Dataset
    *   val observed_ds = ds.observe("my_event", count(lit(1)).as("rc"), count($"error").as("erc"))
    *   observed_ds.writeStream.format("...").start()
    * }}}
    *
    * @group typedrel
    * @since 3.0.0
    */
  def observe(name: String, expr: Column, exprs: Column*): Dataset[T] = transformation(_.observe(name, expr, exprs: _*))
  
  /**
     * Returns a new Dataset sorted by the given expressions.
     * This is an alias of the `sort` function.
     *
     * @group typedrel
     * @since 2.0.0
     */
  def orderBy(sortCol: String, sortCols: String*): Dataset[T] = transformation(_.orderBy(sortCol, sortCols: _*))
  
  /**
     * Returns a new Dataset sorted by the given expressions.
     * This is an alias of the `sort` function.
     *
     * @group typedrel
     * @since 2.0.0
     */
  def orderBy(sortExprs: Column*): Dataset[T] = transformation(_.orderBy(sortExprs: _*))
  
  /**
     * Returns a new Dataset that has exactly `numPartitions` partitions.
     *
     * @group typedrel
     * @since 1.6.0
     */
  def repartition(numPartitions: Int): Dataset[T] = transformation(_.repartition(numPartitions))
  
  /**
     * Returns a new Dataset partitioned by the given partitioning expressions into
     * `numPartitions`. The resulting Dataset is hash partitioned.
     *
     * This is the same operation as "DISTRIBUTE BY" in SQL (Hive QL).
     *
     * @group typedrel
     * @since 2.0.0
     */
  def repartition(numPartitions: Int, partitionExprs: Column*): Dataset[T] = transformation(_.repartition(numPartitions, partitionExprs: _*))
  
  /**
     * Returns a new Dataset partitioned by the given partitioning expressions, using
     * `spark.sql.shuffle.partitions` as number of partitions.
     * The resulting Dataset is hash partitioned.
     *
     * This is the same operation as "DISTRIBUTE BY" in SQL (Hive QL).
     *
     * @group typedrel
     * @since 2.0.0
     */
  def repartition(partitionExprs: Column*): Dataset[T] = transformation(_.repartition(partitionExprs: _*))
  
  /**
     * Returns a new Dataset partitioned by the given partitioning expressions into
     * `numPartitions`. The resulting Dataset is range partitioned.
     *
     * At least one partition-by expression must be specified.
     * When no explicit sort order is specified, "ascending nulls first" is assumed.
     * Note, the rows are not sorted in each partition of the resulting Dataset.
     *
     *
     * Note that due to performance reasons this method uses sampling to estimate the ranges.
     * Hence, the output may not be consistent, since sampling can return different values.
     * The sample size can be controlled by the config
     * `spark.sql.execution.rangeExchange.sampleSizePerPartition`.
     *
     * @group typedrel
     * @since 2.3.0
     */
  def repartitionByRange(numPartitions: Int, partitionExprs: Column*): Dataset[T] = transformation(_.repartitionByRange(numPartitions, partitionExprs: _*))
  
  /**
     * Returns a new Dataset partitioned by the given partitioning expressions, using
     * `spark.sql.shuffle.partitions` as number of partitions.
     * The resulting Dataset is range partitioned.
     *
     * At least one partition-by expression must be specified.
     * When no explicit sort order is specified, "ascending nulls first" is assumed.
     * Note, the rows are not sorted in each partition of the resulting Dataset.
     *
     * Note that due to performance reasons this method uses sampling to estimate the ranges.
     * Hence, the output may not be consistent, since sampling can return different values.
     * The sample size can be controlled by the config
     * `spark.sql.execution.rangeExchange.sampleSizePerPartition`.
     *
     * @group typedrel
     * @since 2.3.0
     */
  def repartitionByRange(partitionExprs: Column*): Dataset[T] = transformation(_.repartitionByRange(partitionExprs: _*))
  
  /**
     * Returns a new [[Dataset]] by sampling a fraction of rows (without replacement),
     * using a user-supplied seed.
     *
     * @param fraction Fraction of rows to generate, range [0.0, 1.0].
     * @param seed Seed for sampling.
     *
     * @note This is NOT guaranteed to provide exactly the fraction of the count
     * of the given [[Dataset]].
     *
     * @group typedrel
     * @since 2.3.0
     */
  def sample(fraction: Double, seed: Long): Dataset[T] = transformation(_.sample(fraction, seed))
  
  /**
     * Returns a new [[Dataset]] by sampling a fraction of rows (without replacement),
     * using a random seed.
     *
     * @param fraction Fraction of rows to generate, range [0.0, 1.0].
     *
     * @note This is NOT guaranteed to provide exactly the fraction of the count
     * of the given [[Dataset]].
     *
     * @group typedrel
     * @since 2.3.0
     */
  def sample(fraction: Double): Dataset[T] = transformation(_.sample(fraction))
  
  /**
     * Returns a new [[Dataset]] by sampling a fraction of rows, using a user-supplied seed.
     *
     * @param withReplacement Sample with replacement or not.
     * @param fraction Fraction of rows to generate, range [0.0, 1.0].
     * @param seed Seed for sampling.
     *
     * @note This is NOT guaranteed to provide exactly the fraction of the count
     * of the given [[Dataset]].
     *
     * @group typedrel
     * @since 1.6.0
     */
  def sample(withReplacement: Boolean, fraction: Double, seed: Long): Dataset[T] = transformation(_.sample(withReplacement, fraction, seed))
  
  /**
     * Returns a new [[Dataset]] by sampling a fraction of rows, using a random seed.
     *
     * @param withReplacement Sample with replacement or not.
     * @param fraction Fraction of rows to generate, range [0.0, 1.0].
     *
     * @note This is NOT guaranteed to provide exactly the fraction of the total count
     * of the given [[Dataset]].
     *
     * @group typedrel
     * @since 1.6.0
     */
  def sample(withReplacement: Boolean, fraction: Double): Dataset[T] = transformation(_.sample(withReplacement, fraction))
  
  /**
     * Returns a new Dataset by computing the given [[Column]] expression for each element.
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
     * Returns a new Dataset by computing the given [[Column]] expressions for each element.
     *
     * @group typedrel
     * @since 1.6.0
     */
  def select[U1, U2](c1: TypedColumn[T, U1], c2: TypedColumn[T, U2]): Dataset[(U1, U2)] = transformation(_.select(c1, c2))
  
  /**
     * Returns a new Dataset by computing the given [[Column]] expressions for each element.
     *
     * @group typedrel
     * @since 1.6.0
     */
  def select[U1, U2, U3](c1: TypedColumn[T, U1], c2: TypedColumn[T, U2], c3: TypedColumn[T, U3]): Dataset[(U1, U2, U3)] = transformation(_.select(c1, c2, c3))
  
  /**
     * Returns a new Dataset by computing the given [[Column]] expressions for each element.
     *
     * @group typedrel
     * @since 1.6.0
     */
  def select[U1, U2, U3, U4](c1: TypedColumn[T, U1], c2: TypedColumn[T, U2], c3: TypedColumn[T, U3], c4: TypedColumn[T, U4]): Dataset[(U1, U2, U3, U4)] = transformation(_.select(c1, c2, c3, c4))
  
  /**
     * Returns a new Dataset by computing the given [[Column]] expressions for each element.
     *
     * @group typedrel
     * @since 1.6.0
     */
  def select[U1, U2, U3, U4, U5](c1: TypedColumn[T, U1], c2: TypedColumn[T, U2], c3: TypedColumn[T, U3], c4: TypedColumn[T, U4], c5: TypedColumn[T, U5]): Dataset[(U1, U2, U3, U4, U5)] = transformation(_.select(c1, c2, c3, c4, c5))
  
  /**
     * Returns a new Dataset sorted by the specified column, all in ascending order.
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
  def sort(sortExprs: Column*): Dataset[T] = transformation(_.sort(sortExprs: _*))
  
  /**
     * Returns a new Dataset with each partition sorted by the given expressions.
     *
     * This is the same operation as "SORT BY" in SQL (Hive QL).
     *
     * @group typedrel
     * @since 2.0.0
     */
  def sortWithinPartitions(sortCol: String, sortCols: String*): Dataset[T] = transformation(_.sortWithinPartitions(sortCol, sortCols: _*))
  
  /**
     * Returns a new Dataset with each partition sorted by the given expressions.
     *
     * This is the same operation as "SORT BY" in SQL (Hive QL).
     *
     * @group typedrel
     * @since 2.0.0
     */
  def sortWithinPartitions(sortExprs: Column*): Dataset[T] = transformation(_.sortWithinPartitions(sortExprs: _*))
  
  /**
     * Returns the content of the Dataset as a Dataset of JSON strings.
     * @since 2.0.0
     */
  def toJSON: Dataset[String] = transformation(_.toJSON)
  
  /**
     * Returns a new Dataset containing union of rows in this Dataset and another Dataset.
     *
     * This is equivalent to `UNION ALL` in SQL. To do a SQL-style set union (that does
     * deduplication of elements), use this function followed by a [[distinct]].
     *
     * Also as standard in SQL, this function resolves columns by position (not by name):
     *
     * {{{
     *   val df1 = Seq((1, 2, 3)).toDF("col0", "col1", "col2")
     *   val df2 = Seq((4, 5, 6)).toDF("col1", "col2", "col0")
     *   df1.union(df2).show
     *
     *   // output:
     *   // +----+----+----+
     *   // |col0|col1|col2|
     *   // +----+----+----+
     *   // |   1|   2|   3|
     *   // |   4|   5|   6|
     *   // +----+----+----+
     * }}}
     *
     * Notice that the column positions in the schema aren't necessarily matched with the
     * fields in the strongly typed objects in a Dataset. This function resolves columns
     * by their positions in the schema, not the fields in the strongly typed objects. Use
     * [[unionByName]] to resolve columns by field name in the typed objects.
     *
     * @group typedrel
     * @since 2.0.0
     */
  def union(other: Dataset[T]): Dataset[T] = transformation(_.union(other))
  
  /**
     * Returns a new Dataset containing union of rows in this Dataset and another Dataset.
     * This is an alias for `union`.
     *
     * This is equivalent to `UNION ALL` in SQL. To do a SQL-style set union (that does
     * deduplication of elements), use this function followed by a [[distinct]].
     *
     * Also as standard in SQL, this function resolves columns by position (not by name).
     *
     * @group typedrel
     * @since 2.0.0
     */
  def unionAll(other: Dataset[T]): Dataset[T] = transformation(_.unionAll(other))
  
  /**
     * Returns a new Dataset containing union of rows in this Dataset and another Dataset.
     *
     * This is different from both `UNION ALL` and `UNION DISTINCT` in SQL. To do a SQL-style set
     * union (that does deduplication of elements), use this function followed by a [[distinct]].
     *
     * The difference between this function and [[union]] is that this function
     * resolves columns by name (not by position):
     *
     * {{{
     *   val df1 = Seq((1, 2, 3)).toDF("col0", "col1", "col2")
     *   val df2 = Seq((4, 5, 6)).toDF("col1", "col2", "col0")
     *   df1.unionByName(df2).show
     *
     *   // output:
     *   // +----+----+----+
     *   // |col0|col1|col2|
     *   // +----+----+----+
     *   // |   1|   2|   3|
     *   // |   6|   4|   5|
     *   // +----+----+----+
     * }}}
     *
     * @group typedrel
     * @since 2.3.0
     */
  def unionByName(other: Dataset[T]): Dataset[T] = transformation(_.unionByName(other))
  
  /**
     * Returns a new Dataset containing union of rows in this Dataset and another Dataset.
     *
     * The difference between this function and [[union]] is that this function
     * resolves columns by name (not by position).
     *
     * When the parameter `allowMissingColumns` is `true`, the set of column names
     * in this and other `Dataset` can differ; missing columns will be filled with null.
     * Further, the missing columns of this `Dataset` will be added at the end
     * in the schema of the union result:
     *
     * {{{
     *   val df1 = Seq((1, 2, 3)).toDF("col0", "col1", "col2")
     *   val df2 = Seq((4, 5, 6)).toDF("col1", "col0", "col3")
     *   df1.unionByName(df2, true).show
     *
     *   // output: "col3" is missing at left df1 and added at the end of schema.
     *   // +----+----+----+----+
     *   // |col0|col1|col2|col3|
     *   // +----+----+----+----+
     *   // |   1|   2|   3|null|
     *   // |   5|   4|null|   6|
     *   // +----+----+----+----+
     *
     *   df2.unionByName(df1, true).show
     *
     *   // output: "col2" is missing at left df2 and added at the end of schema.
     *   // +----+----+----+----+
     *   // |col1|col0|col3|col2|
     *   // +----+----+----+----+
     *   // |   4|   5|   6|null|
     *   // |   2|   1|null|   3|
     *   // +----+----+----+----+
     * }}}
     *
     * Note that `allowMissingColumns` supports nested column in struct types. Missing nested columns
     * of struct columns with the same name will also be filled with null values and added to the end
     * of struct. This currently does not support nested columns in array and map types.
     *
     * @group typedrel
     * @since 3.1.0
     */
  def unionByName(other: Dataset[T], allowMissingColumns: Boolean): Dataset[T] = transformation(_.unionByName(other, allowMissingColumns))
  
  /**
     * Filters rows using the given condition. This is an alias for `filter`.
     * {{{
     *   // The following are equivalent:
     *   peopleDs.filter($"age" > 15)
     *   peopleDs.where($"age" > 15)
     * }}}
     *
     * @group typedrel
     * @since 1.6.0
     */
  def where(condition: Column): Dataset[T] = transformation(_.where(condition))
  
  /**
     * Filters rows using the given SQL expression.
     * {{{
     *   peopleDs.where("age > 15")
     * }}}
     *
     * @group typedrel
     * @since 1.6.0
     */
  def where(conditionExpr: String): Dataset[T] = transformation(_.where(conditionExpr))
  
  /**
     * Defines an event time watermark for this [[Dataset]]. A watermark tracks a point in time
     * before which we assume no more late data is going to arrive.
     *
     * Spark will use this watermark for several purposes:
     * <ul>
     *   <li>To know when a given time window aggregation can be finalized and thus can be emitted
     *   when using output modes that do not allow updates.</li>
     *   <li>To minimize the amount of state that we need to keep for on-going aggregations,
     *    `mapGroupsWithState` and `dropDuplicates` operators.</li>
     * </ul>
     *  The current watermark is computed by looking at the `MAX(eventTime)` seen across
     *  all of the partitions in the query minus a user specified `delayThreshold`.  Due to the cost
     *  of coordinating this value across partitions, the actual watermark used is only guaranteed
     *  to be at least `delayThreshold` behind the actual event time.  In some cases we may still
     *  process records that arrive more than `delayThreshold` late.
     *
     * @param eventTime the name of the column that contains the event time of the row.
     * @param delayThreshold the minimum delay to wait to data to arrive late, relative to the latest
     *                       record that has been processed in the form of an interval
     *                       (e.g. "1 minute" or "5 hours"). NOTE: This should not be negative.
     *
     * @group streaming
     * @since 2.1.0
     */
  // We only accept an existing column name, not a derived column here as a watermark that is
  // defined on a derived column cannot referenced elsewhere in the plan.
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
   * [[org.apache.spark.sql.Dataset.agg]]
   * [[org.apache.spark.sql.Dataset.apply]]
   * [[org.apache.spark.sql.Dataset.col]]
   * [[org.apache.spark.sql.Dataset.colRegex]]
   * [[org.apache.spark.sql.Dataset.collectAsList]]
   * [[org.apache.spark.sql.Dataset.columns]]
   * [[org.apache.spark.sql.Dataset.createGlobalTempView]]
   * [[org.apache.spark.sql.Dataset.createOrReplaceGlobalTempView]]
   * [[org.apache.spark.sql.Dataset.createOrReplaceTempView]]
   * [[org.apache.spark.sql.Dataset.createTempView]]
   * [[org.apache.spark.sql.Dataset.crossJoin]]
   * [[org.apache.spark.sql.Dataset.cube]]
   * [[org.apache.spark.sql.Dataset.describe]]
   * [[org.apache.spark.sql.Dataset.drop]]
   * [[org.apache.spark.sql.Dataset.dtypes]]
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
   * [[org.apache.spark.sql.Dataset.join]]
   * [[org.apache.spark.sql.Dataset.map]]
   * [[org.apache.spark.sql.Dataset.mapPartitions]]
   * [[org.apache.spark.sql.Dataset.na]]
   * [[org.apache.spark.sql.Dataset.printSchema]]
   * [[org.apache.spark.sql.Dataset.randomSplitAsList]]
   * [[org.apache.spark.sql.Dataset.reduce]]
   * [[org.apache.spark.sql.Dataset.registerTempTable]]
   * [[org.apache.spark.sql.Dataset.rollup]]
   * [[org.apache.spark.sql.Dataset.schema]]
   * [[org.apache.spark.sql.Dataset.select]]
   * [[org.apache.spark.sql.Dataset.selectExpr]]
   * [[org.apache.spark.sql.Dataset.show]]
   * [[org.apache.spark.sql.Dataset.stat]]
   * [[org.apache.spark.sql.Dataset.storageLevel]]
   * [[org.apache.spark.sql.Dataset.summary]]
   * [[org.apache.spark.sql.Dataset.tail]]
   * [[org.apache.spark.sql.Dataset.takeAsList]]
   * [[org.apache.spark.sql.Dataset.toDF]]
   * [[org.apache.spark.sql.Dataset.toString]]
   * [[org.apache.spark.sql.Dataset.withColumn]]
   * [[org.apache.spark.sql.Dataset.withColumnRenamed]]
   * [[org.apache.spark.sql.Dataset.write]]
   * [[org.apache.spark.sql.Dataset.writeStream]]
   * [[org.apache.spark.sql.Dataset.writeTo]]
   */
}
