package zio.spark.internal.codegen


import scala.reflect._

import scala.io.Codec

import org.apache.spark.partial.{PartialResult, BoundedDouble}
import org.apache.spark.rdd.{RDD => UnderlyingRDD, RDDBarrier, PartitionCoalescer}
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Partition, TaskContext, Dependency, Partitioner}
import org.apache.hadoop.io.compress.CompressionCodec

import zio.Task
import zio.spark.impure.Impure
import zio.spark.impure.Impure.ImpureBox
import zio.spark.rdd.RDD

import scala.collection.Map



@SuppressWarnings(Array("scalafix:DisableSyntax.defaultArgs", "scalafix:DisableSyntax.null"))
abstract class BaseRDD[T](underlyingRDD: ImpureBox[UnderlyingRDD[T]]) extends Impure[UnderlyingRDD[T]](underlyingRDD) {
  import underlyingRDD._

  // scalafix:off
  private implicit def arrayToSeq1[U](x: RDD[Array[U]]): RDD[Seq[U]] = x.map(_.toIndexedSeq)
  private implicit def arrayToSeq2[U](x: UnderlyingRDD[Array[U]]): UnderlyingRDD[Seq[U]] = x.map(_.toIndexedSeq)
  private implicit def lift[U](x:UnderlyingRDD[U]):RDD[U] = RDD(x)
  private implicit def escape[U](x:RDD[U]):UnderlyingRDD[U] = x.underlyingRDD.succeedNow(v => v)
  private implicit def iteratorConversion[U](iterator: java.util.Iterator[U]):Iterator[U] = scala.collection.JavaConverters.asScalaIteratorConverter(iterator).asScala
  
  @inline private def noOrdering[U]: Ordering[U] = null
  // scalafix:on
  
  /** Applies an action to the underlying RDD. */
  def action[U](f: UnderlyingRDD[T] => U): Task[U] = attemptBlocking(f)

  /** Applies a transformation to the underlying RDD. */
  def transformation[U](f: UnderlyingRDD[T] => UnderlyingRDD[U]): RDD[U] = succeedNow(f.andThen(x => RDD(x)))

  /**
     * :: Experimental ::
     * Marks the current stage as a barrier stage, where Spark must launch all tasks together.
     * In case of a task failure, instead of only restarting the failed task, Spark will abort the
     * entire stage and re-launch all tasks for this stage.
     * The barrier execution mode feature is experimental and it only handles limited scenarios.
     * Please read the linked SPIP and design docs to understand the limitations and future plans.
     * @return an [[RDDBarrier]] instance that provides actions within a barrier stage
     * @see [[org.apache.spark.BarrierTaskContext]]
     * @see <a href="https://jira.apache.org/jira/browse/SPARK-24374">SPIP: Barrier Execution Mode</a>
     * @see <a href="https://jira.apache.org/jira/browse/SPARK-24582">Design Doc</a>
     */
  def barrier: RDDBarrier[T] = succeedNow(_.barrier())
  
  /**
     * Returns the number of partitions of this RDD.
     */
  def getNumPartitions: Int = succeedNow(_.getNumPartitions)
  
  /**
     * Get the array of partitions of this RDD, taking into account whether the
     * RDD is checkpointed or not.
     */
  def partitions: Seq[Partition] = succeedNow(_.partitions)
  
  /**
     * Get the preferred locations of a partition, taking into account whether the
     * RDD is checkpointed.
     */
  def preferredLocations(split: Partition): Seq[String] = succeedNow(_.preferredLocations(split))
  
  /** A description of this RDD and its recursive dependencies for debugging. */
  def toDebugString: String = succeedNow(_.toDebugString)
  
  //===============
  
  /**
     * Aggregate the elements of each partition, and then the results for all the partitions, using
     * given combine functions and a neutral "zero value". This function can return a different result
     * type, U, than the type of this RDD, T. Thus, we need one operation for merging a T into an U
     * and one operation for merging two U's, as in scala.TraversableOnce. Both of these functions are
     * allowed to modify and return their first argument instead of creating a new U to avoid memory
     * allocation.
     *
     * @param zeroValue the initial value for the accumulated result of each partition for the
     *                  `seqOp` operator, and also the initial value for the combine results from
     *                  different partitions for the `combOp` operator - this will typically be the
     *                  neutral element (e.g. `Nil` for list concatenation or `0` for summation)
     * @param seqOp an operator used to accumulate results within a partition
     * @param combOp an associative operator used to combine results from different partitions
     */
  def aggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): Task[U] = action(_.aggregate(zeroValue)(seqOp, combOp))
  
  /**
     * Return an array that contains all of the elements in this RDD.
     *
     * @note This method should only be used if the resulting array is expected to be small, as
     * all the data is loaded into the driver's memory.
     */
  def collect: Task[Seq[T]] = action(_.collect())
  
  /**
     * Return the number of elements in the RDD.
     */
  def count: Task[Long] = action(_.count())
  
  /**
     * Approximate version of count() that returns a potentially incomplete result
     * within a timeout, even if not all tasks have finished.
     *
     * The confidence is the probability that the error bounds of the result will
     * contain the true value. That is, if countApprox were called repeatedly
     * with confidence 0.9, we would expect 90% of the results to contain the
     * true count. The confidence must be in the range [0,1] or an exception will
     * be thrown.
     *
     * @param timeout maximum time to wait for the job, in milliseconds
     * @param confidence the desired statistical confidence in the result
     * @return a potentially incomplete result, with error bounds
     */
  def countApprox(timeout: Long, confidence: Double = 0.95): Task[PartialResult[BoundedDouble]] = action(_.countApprox(timeout, confidence))
  
  /**
     * Return approximate number of distinct elements in the RDD.
     *
     * The algorithm used is based on streamlib's implementation of "HyperLogLog in Practice:
     * Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm", available
     * <a href="https://doi.org/10.1145/2452376.2452456">here</a>.
     *
     * The relative accuracy is approximately `1.054 / sqrt(2^p)`. Setting a nonzero (`sp` is greater
     * than `p`) would trigger sparse representation of registers, which may reduce the memory
     * consumption and increase accuracy when the cardinality is small.
     *
     * @param p The precision value for the normal set.
     *          `p` must be a value between 4 and `sp` if `sp` is not zero (32 max).
     * @param sp The precision value for the sparse set, between 0 and 32.
     *           If `sp` equals 0, the sparse representation is skipped.
     */
  def countApproxDistinct(p: Int, sp: Int): Task[Long] = action(_.countApproxDistinct(p, sp))
  
  /**
     * Return approximate number of distinct elements in the RDD.
     *
     * The algorithm used is based on streamlib's implementation of "HyperLogLog in Practice:
     * Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm", available
     * <a href="https://doi.org/10.1145/2452376.2452456">here</a>.
     *
     * @param relativeSD Relative accuracy. Smaller values create counters that require more space.
     *                   It must be greater than 0.000017.
     */
  def countApproxDistinct(relativeSD: Double = 0.05): Task[Long] = action(_.countApproxDistinct(relativeSD))
  
  /**
     * Return the count of each unique value in this RDD as a local map of (value, count) pairs.
     *
     * @note This method should only be used if the resulting map is expected to be small, as
     * the whole thing is loaded into the driver's memory.
     * To handle very large results, consider using
     *
     * {{{
     * rdd.map(x => (x, 1L)).reduceByKey(_ + _)
     * }}}
     *
     * , which returns an RDD[T, Long] instead of a map.
     */
  def countByValue(implicit ord: Ordering[T] = noOrdering): Task[Map[T, Long]] = action(_.countByValue())
  
  /**
     * Approximate version of countByValue().
     *
     * @param timeout maximum time to wait for the job, in milliseconds
     * @param confidence the desired statistical confidence in the result
     * @return a potentially incomplete result, with error bounds
     */
  def countByValueApprox(timeout: Long, confidence: Double = 0.95)(implicit ord: Ordering[T] = noOrdering): Task[PartialResult[Map[T, BoundedDouble]]] = action(_.countByValueApprox(timeout, confidence))
  
  /**
     * Return the first element in this RDD.
     */
  def first: Task[T] = action(_.first())
  
  /**
     * Aggregate the elements of each partition, and then the results for all the partitions, using a
     * given associative function and a neutral "zero value". The function
     * op(t1, t2) is allowed to modify t1 and return it as its result value to avoid object
     * allocation; however, it should not modify t2.
     *
     * This behaves somewhat differently from fold operations implemented for non-distributed
     * collections in functional languages like Scala. This fold operation may be applied to
     * partitions individually, and then fold those results into the final result, rather than
     * apply the fold to each element sequentially in some defined ordering. For functions
     * that are not commutative, the result may differ from that of a fold applied to a
     * non-distributed collection.
     *
     * @param zeroValue the initial value for the accumulated result of each partition for the `op`
     *                  operator, and also the initial value for the combine results from different
     *                  partitions for the `op` operator - this will typically be the neutral
     *                  element (e.g. `Nil` for list concatenation or `0` for summation)
     * @param op an operator used to both accumulate results within a partition and combine results
     *                  from different partitions
     */
  def fold(zeroValue: T)(op: (T, T) => T): Task[T] = action(_.fold(zeroValue)(op))
  
  // Actions (launch a job to return a value to the user program)
  /**
     * Applies a function f to all elements of this RDD.
     */
  def foreach(f: T => Unit): Task[Unit] = action(_.foreach(f))
  
  /**
     * Applies a function f to each partition of this RDD.
     */
  def foreachPartition(f: Iterator[T] => Unit): Task[Unit] = action(_.foreachPartition(f))
  
  /**
     * @note Due to complications in the internal implementation, this method will raise an
     * exception if called on an RDD of `Nothing` or `Null`. This may be come up in practice
     * because, for example, the type of `parallelize(Seq())` is `RDD[Nothing]`.
     * (`parallelize(Seq())` should be avoided anyway in favor of `parallelize(Seq[T]())`.)
     * @return true if and only if the RDD contains no elements at all. Note that an RDD
     *         may be empty even when it has at least 1 partition.
     */
  def isEmpty: Task[Boolean] = action(_.isEmpty())
  
  /**
     * Internal method to this RDD; will read from cache if applicable, or otherwise compute it.
     * This should ''not'' be called by users directly, but is available for implementers of custom
     * subclasses of RDD.
     */
  def iterator(split: Partition, context: TaskContext): Task[Iterator[T]] = action(_.iterator(split, context))
  
  /**
     * Returns the max of this RDD as defined by the implicit Ordering[T].
     * @return the maximum element of the RDD
     * */
  def max(implicit ord: Ordering[T]): Task[T] = action(_.max())
  
  /**
     * Returns the min of this RDD as defined by the implicit Ordering[T].
     * @return the minimum element of the RDD
     * */
  def min(implicit ord: Ordering[T]): Task[T] = action(_.min())
  
  /**
     * Reduces the elements of this RDD using the specified commutative and
     * associative binary operator.
     */
  def reduce(f: (T, T) => T): Task[T] = action(_.reduce(f))
  
  /**
     * Save this RDD as a SequenceFile of serialized objects.
     */
  def saveAsObjectFile(path: String): Task[Unit] = action(_.saveAsObjectFile(path))
  
  /**
     * Save this RDD as a text file, using string representations of elements.
     */
  def saveAsTextFile(path: String): Task[Unit] = action(_.saveAsTextFile(path))
  
  /**
     * Save this RDD as a compressed text file, using string representations of elements.
     */
  def saveAsTextFile(path: String, codec: Class[_ <: CompressionCodec]): Task[Unit] = action(_.saveAsTextFile(path, codec))
  
  /**
     * Take the first num elements of the RDD. It works by first scanning one partition, and use the
     * results from that partition to estimate the number of additional partitions needed to satisfy
     * the limit.
     *
     * @note This method should only be used if the resulting array is expected to be small, as
     * all the data is loaded into the driver's memory.
     *
     * @note Due to complications in the internal implementation, this method will raise
     * an exception if called on an RDD of `Nothing` or `Null`.
     */
  def take(num: Int): Task[Seq[T]] = action(_.take(num))
  
  /**
     * Returns the first k (smallest) elements from this RDD as defined by the specified
     * implicit Ordering[T] and maintains the ordering. This does the opposite of [[top]].
     * For example:
     * {{{
     *   sc.parallelize(Seq(10, 4, 2, 12, 3)).takeOrdered(1)
     *   // returns Array(2)
     *
     *   sc.parallelize(Seq(2, 3, 4, 5, 6)).takeOrdered(2)
     *   // returns Array(2, 3)
     * }}}
     *
     * @note This method should only be used if the resulting array is expected to be small, as
     * all the data is loaded into the driver's memory.
     *
     * @param num k, the number of elements to return
     * @param ord the implicit ordering for T
     * @return an array of top elements
     */
  def takeOrdered(num: Int)(implicit ord: Ordering[T]): Task[Seq[T]] = action(_.takeOrdered(num))
  
  /**
     * Return a fixed-size sampled subset of this RDD in an array
     *
     * @param withReplacement whether sampling is done with replacement
     * @param num size of the returned sample
     * @param seed seed for the random number generator
     * @return sample of specified size in an array
     *
     * @note this method should only be used if the resulting array is expected to be small, as
     * all the data is loaded into the driver's memory.
     */
  def takeSample(withReplacement: Boolean, num: Int, seed: Long): Task[Seq[T]] = action(_.takeSample(withReplacement, num, seed))
  
  /**
     * Return an iterator that contains all of the elements in this RDD.
     *
     * The iterator will consume as much memory as the largest partition in this RDD.
     *
     * @note This results in multiple Spark jobs, and if the input RDD is the result
     * of a wide transformation (e.g. join with different partitioners), to avoid
     * recomputing the input RDD should be cached first.
     */
  def toLocalIterator: Task[Iterator[T]] = action(_.toLocalIterator)
  
  /**
     * Returns the top k (largest) elements from this RDD as defined by the specified
     * implicit Ordering[T] and maintains the ordering. This does the opposite of
     * [[takeOrdered]]. For example:
     * {{{
     *   sc.parallelize(Seq(10, 4, 2, 12, 3)).top(1)
     *   // returns Array(12)
     *
     *   sc.parallelize(Seq(2, 3, 4, 5, 6)).top(2)
     *   // returns Array(6, 5)
     * }}}
     *
     * @note This method should only be used if the resulting array is expected to be small, as
     * all the data is loaded into the driver's memory.
     *
     * @param num k, the number of top elements to return
     * @param ord the implicit ordering for T
     * @return an array of top elements
     */
  def top(num: Int)(implicit ord: Ordering[T]): Task[Seq[T]] = action(_.top(num))
  
  /**
     * Aggregates the elements of this RDD in a multi-level tree pattern.
     * This method is semantically identical to [[org.apache.spark.rdd.RDD#aggregate]].
     *
     * @param depth suggested depth of the tree (default: 2)
     */
  def treeAggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U, depth: Int = 2): Task[U] = action(_.treeAggregate(zeroValue)(seqOp, combOp, depth))
  
  /**
     * Reduces the elements of this RDD in a multi-level tree pattern.
     *
     * @param depth suggested depth of the tree (default: 2)
     * @see [[org.apache.spark.rdd.RDD#reduce]]
     */
  def treeReduce(f: (T, T) => T, depth: Int = 2): Task[T] = action(_.treeReduce(f, depth))
  
  //===============
  
  /**
     * Persist this RDD with the default storage level (`MEMORY_ONLY`).
     */
  def cache: Task[RDD[T]] = action(_.cache())
  
  /**
     * Mark this RDD for checkpointing. It will be saved to a file inside the checkpoint
     * directory set with `SparkContext#setCheckpointDir` and all references to its parent
     * RDDs will be removed. This function must be called before any job has been
     * executed on this RDD. It is strongly recommended that this RDD is persisted in
     * memory, otherwise saving it on a file will require recomputation.
     */
  def checkpoint: Task[Unit] = action(_.checkpoint())
  
  /**
     * Get the list of dependencies of this RDD, taking into account whether the
     * RDD is checkpointed or not.
     */
  def dependencies: Task[Seq[Dependency[_]]] = action(_.dependencies)
  
  /**
     * Gets the name of the directory to which this RDD was checkpointed.
     * This is not defined if the RDD is checkpointed locally.
     */
  def getCheckpointFile: Task[Option[String]] = action(_.getCheckpointFile)
  
  /**
     * Get the ResourceProfile specified with this RDD or null if it wasn't specified.
     * @return the user specified ResourceProfile or null (for Java compatibility) if
     *         none was specified
     */
  def getResourceProfile: Task[ResourceProfile] = action(_.getResourceProfile())
  
  /** Get the RDD's current storage level, or StorageLevel.NONE if none is set. */
  def getStorageLevel: Task[StorageLevel] = action(_.getStorageLevel)
  
  /**
     * Return whether this RDD is checkpointed and materialized, either reliably or locally.
     */
  def isCheckpointed: Task[Boolean] = action(_.isCheckpointed)
  
  /**
     * Mark this RDD for local checkpointing using Spark's existing caching layer.
     *
     * This method is for users who wish to truncate RDD lineages while skipping the expensive
     * step of replicating the materialized data in a reliable distributed file system. This is
     * useful for RDDs with long lineages that need to be truncated periodically (e.g. GraphX).
     *
     * Local checkpointing sacrifices fault-tolerance for performance. In particular, checkpointed
     * data is written to ephemeral local storage in the executors instead of to a reliable,
     * fault-tolerant storage. The effect is that if an executor fails during the computation,
     * the checkpointed data may no longer be accessible, causing an irrecoverable job failure.
     *
     * This is NOT safe to use with dynamic allocation, which removes executors along
     * with their cached blocks. If you must use both features, you are advised to set
     * `spark.dynamicAllocation.cachedExecutorIdleTimeout` to a high value.
     *
     * The checkpoint directory set through `SparkContext#setCheckpointDir` is not used.
     */
  def localCheckpoint: Task[RDD[T]] = action(_.localCheckpoint())
  
  /**
     * Set this RDD's storage level to persist its values across operations after the first time
     * it is computed. This can only be used to assign a new storage level if the RDD does not
     * have a storage level set yet. Local checkpointing is an exception.
     */
  def persist(newLevel: StorageLevel): Task[RDD[T]] = action(_.persist(newLevel))
  
  /**
     * Persist this RDD with the default storage level (`MEMORY_ONLY`).
     */
  def persist: Task[RDD[T]] = action(_.persist())
  
  /**
     * Mark the RDD as non-persistent, and remove all blocks for it from memory and disk.
     *
     * @param blocking Whether to block until all blocks are deleted (default: false)
     * @return This RDD.
     */
  def unpersist(blocking: Boolean = false): Task[RDD[T]] = action(_.unpersist(blocking))
  
  //===============
  
  /**
     * Return the union of this RDD and another one. Any identical elements will appear multiple
     * times (use `.distinct()` to eliminate them).
     */
  def ++(other: RDD[T]): RDD[T] = transformation(_.++(other))
  
  /**
     * Return the Cartesian product of this RDD and another one, that is, the RDD of all pairs of
     * elements (a, b) where a is in `this` and b is in `other`.
     */
  def cartesian[U: ClassTag](other: RDD[U]): RDD[(T, U)] = transformation(_.cartesian(other))
  
  /**
     * Return a new RDD that is reduced into `numPartitions` partitions.
     *
     * This results in a narrow dependency, e.g. if you go from 1000 partitions
     * to 100 partitions, there will not be a shuffle, instead each of the 100
     * new partitions will claim 10 of the current partitions. If a larger number
     * of partitions is requested, it will stay at the current number of partitions.
     *
     * However, if you're doing a drastic coalesce, e.g. to numPartitions = 1,
     * this may result in your computation taking place on fewer nodes than
     * you like (e.g. one node in the case of numPartitions = 1). To avoid this,
     * you can pass shuffle = true. This will add a shuffle step, but means the
     * current upstream partitions will be executed in parallel (per whatever
     * the current partitioning is).
     *
     * @note With shuffle = true, you can actually coalesce to a larger number
     * of partitions. This is useful if you have a small number of partitions,
     * say 100, potentially with a few partitions being abnormally large. Calling
     * coalesce(1000, shuffle = true) will result in 1000 partitions with the
     * data distributed using a hash partitioner. The optional partition coalescer
     * passed in must be serializable.
     */
  def coalesce(numPartitions: Int, shuffle: Boolean = false, partitionCoalescer: Option[PartitionCoalescer] = Option.empty)(implicit ord: Ordering[T] = noOrdering): RDD[T] = transformation(_.coalesce(numPartitions, shuffle, partitionCoalescer))
  
  /**
     * Return a new RDD containing the distinct elements in this RDD.
     */
  def distinct(numPartitions: Int)(implicit ord: Ordering[T] = noOrdering): RDD[T] = transformation(_.distinct(numPartitions))
  
  /**
     * Return a new RDD containing the distinct elements in this RDD.
     */
  def distinct: RDD[T] = transformation(_.distinct())
  
  /**
     * Return a new RDD containing only the elements that satisfy a predicate.
     */
  def filter(f: T => Boolean): RDD[T] = transformation(_.filter(f))
  
  /**
     *  Return a new RDD by first applying a function to all elements of this
     *  RDD, and then flattening the results.
     */
  def flatMap[U: ClassTag](f: T => TraversableOnce[U]): RDD[U] = transformation(_.flatMap(f))
  
  /**
     * Return an RDD created by coalescing all elements within each partition into an array.
     */
  def glom: RDD[Seq[T]] = transformation(_.glom())
  
  /**
     * Return an RDD of grouped items. Each group consists of a key and a sequence of elements
     * mapping to that key. The ordering of elements within each group is not guaranteed, and
     * may even differ each time the resulting RDD is evaluated.
     *
     * @note This operation may be very expensive. If you are grouping in order to perform an
     * aggregation (such as a sum or average) over each key, using `PairRDDFunctions.aggregateByKey`
     * or `PairRDDFunctions.reduceByKey` will provide much better performance.
     */
  def groupBy[K](f: T => K)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])] = transformation(_.groupBy(f))
  
  /**
     * Return an RDD of grouped elements. Each group consists of a key and a sequence of elements
     * mapping to that key. The ordering of elements within each group is not guaranteed, and
     * may even differ each time the resulting RDD is evaluated.
     *
     * @note This operation may be very expensive. If you are grouping in order to perform an
     * aggregation (such as a sum or average) over each key, using `PairRDDFunctions.aggregateByKey`
     * or `PairRDDFunctions.reduceByKey` will provide much better performance.
     */
  def groupBy[K](f: T => K, numPartitions: Int)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])] = transformation(_.groupBy(f, numPartitions))
  
  /**
     * Return an RDD of grouped items. Each group consists of a key and a sequence of elements
     * mapping to that key. The ordering of elements within each group is not guaranteed, and
     * may even differ each time the resulting RDD is evaluated.
     *
     * @note This operation may be very expensive. If you are grouping in order to perform an
     * aggregation (such as a sum or average) over each key, using `PairRDDFunctions.aggregateByKey`
     * or `PairRDDFunctions.reduceByKey` will provide much better performance.
     */
  def groupBy[K](f: T => K, p: Partitioner)(implicit kt: ClassTag[K], ord: Ordering[K] = noOrdering): RDD[(K, Iterable[T])] = transformation(_.groupBy(f, p))
  
  /**
     * Return the intersection of this RDD and another one. The output will not contain any duplicate
     * elements, even if the input RDDs did.
     *
     * @note This method performs a shuffle internally.
     */
  def intersection(other: RDD[T]): RDD[T] = transformation(_.intersection(other))
  
  /**
     * Return the intersection of this RDD and another one. The output will not contain any duplicate
     * elements, even if the input RDDs did.
     *
     * @note This method performs a shuffle internally.
     *
     * @param partitioner Partitioner to use for the resulting RDD
     */
  def intersection(other: RDD[T], partitioner: Partitioner)(implicit ord: Ordering[T] = noOrdering): RDD[T] = transformation(_.intersection(other, partitioner))
  
  /**
     * Return the intersection of this RDD and another one. The output will not contain any duplicate
     * elements, even if the input RDDs did.  Performs a hash partition across the cluster
     *
     * @note This method performs a shuffle internally.
     *
     * @param numPartitions How many partitions to use in the resulting RDD
     */
  def intersection(other: RDD[T], numPartitions: Int): RDD[T] = transformation(_.intersection(other, numPartitions))
  
  /**
     * Creates tuples of the elements in this RDD by applying `f`.
     */
  def keyBy[K](f: T => K): RDD[(K, T)] = transformation(_.keyBy(f))
  
  // Transformations (return a new RDD)
  /**
     * Return a new RDD by applying a function to all elements of this RDD.
     */
  def map[U: ClassTag](f: T => U): RDD[U] = transformation(_.map(f))
  
  /**
     * Return a new RDD by applying a function to each partition of this RDD.
     *
     * `preservesPartitioning` indicates whether the input function preserves the partitioner, which
     * should be `false` unless this is a pair RDD and the input function doesn't modify the keys.
     */
  def mapPartitions[U: ClassTag](f: Iterator[T] => Iterator[U], preservesPartitioning: Boolean = false): RDD[U] = transformation(_.mapPartitions(f, preservesPartitioning))
  
  /**
     * Return a new RDD by applying a function to each partition of this RDD, while tracking the index
     * of the original partition.
     *
     * `preservesPartitioning` indicates whether the input function preserves the partitioner, which
     * should be `false` unless this is a pair RDD and the input function doesn't modify the keys.
     */
  def mapPartitionsWithIndex[U: ClassTag](f: (Int, Iterator[T]) => Iterator[U], preservesPartitioning: Boolean = false): RDD[U] = transformation(_.mapPartitionsWithIndex(f, preservesPartitioning))
  
  /**
     * Return an RDD created by piping elements to a forked external process.
     */
  def pipe(command: String): RDD[String] = transformation(_.pipe(command))
  
  /**
     * Return an RDD created by piping elements to a forked external process.
     */
  def pipe(command: String, env: Map[String, String]): RDD[String] = transformation(_.pipe(command, env))
  
  /**
     * Return an RDD created by piping elements to a forked external process. The resulting RDD
     * is computed by executing the given process once per partition. All elements
     * of each input partition are written to a process's stdin as lines of input separated
     * by a newline. The resulting partition consists of the process's stdout output, with
     * each line of stdout resulting in one element of the output partition. A process is invoked
     * even for empty partitions.
     *
     * The print behavior can be customized by providing two functions.
     *
     * @param command command to run in forked process.
     * @param env environment variables to set.
     * @param printPipeContext Before piping elements, this function is called as an opportunity
     *                         to pipe context data. Print line function (like out.println) will be
     *                         passed as printPipeContext's parameter.
     * @param printRDDElement Use this function to customize how to pipe elements. This function
     *                        will be called with each RDD element as the 1st parameter, and the
     *                        print line function (like out.println()) as the 2nd parameter.
     *                        An example of pipe the RDD data of groupBy() in a streaming way,
     *                        instead of constructing a huge String to concat all the elements:
     *                        {{{
     *                        def printRDDElement(record:(String, Seq[String]), f:String=>Unit) =
     *                          for (e <- record._2) {f(e)}
     *                        }}}
     * @param separateWorkingDir Use separate working directories for each task.
     * @param bufferSize Buffer size for the stdin writer for the piped process.
     * @param encoding Char encoding used for interacting (via stdin, stdout and stderr) with
     *                 the piped process
     * @return the result RDD
     */
  def pipe(command: Seq[String], env: Map[String, String] = Map(), printPipeContext: (String => Unit) => Unit = null, printRDDElement: (T, String => Unit) => Unit = null, separateWorkingDir: Boolean = false, bufferSize: Int = 8192, encoding: String = Codec.defaultCharsetCodec.name): RDD[String] = transformation(_.pipe(command, env, printPipeContext, printRDDElement, separateWorkingDir, bufferSize, encoding))
  
  /**
     * Return a new RDD that has exactly numPartitions partitions.
     *
     * Can increase or decrease the level of parallelism in this RDD. Internally, this uses
     * a shuffle to redistribute data.
     *
     * If you are decreasing the number of partitions in this RDD, consider using `coalesce`,
     * which can avoid performing a shuffle.
     */
  def repartition(numPartitions: Int)(implicit ord: Ordering[T] = noOrdering): RDD[T] = transformation(_.repartition(numPartitions))
  
  /**
     * Return a sampled subset of this RDD.
     *
     * @param withReplacement can elements be sampled multiple times (replaced when sampled out)
     * @param fraction expected size of the sample as a fraction of this RDD's size
     *  without replacement: probability that each element is chosen; fraction must be [0, 1]
     *  with replacement: expected number of times each element is chosen; fraction must be greater
     *  than or equal to 0
     * @param seed seed for the random number generator
     *
     * @note This is NOT guaranteed to provide exactly the fraction of the count
     * of the given [[RDD]].
     */
  def sample(withReplacement: Boolean, fraction: Double, seed: Long): RDD[T] = transformation(_.sample(withReplacement, fraction, seed))
  
  /**
     * Return this RDD sorted by the given key function.
     */
  def sortBy[K](f: (T) => K, ascending: Boolean = true, numPartitions: Int = this.partitions.length)(implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T] = transformation(_.sortBy(f, ascending, numPartitions))
  
  /**
     * Return an RDD with the elements from `this` that are not in `other`.
     *
     * Uses `this` partitioner/partition size, because even if `other` is huge, the resulting
     * RDD will be &lt;= us.
     */
  def subtract(other: RDD[T]): RDD[T] = transformation(_.subtract(other))
  
  /**
     * Return an RDD with the elements from `this` that are not in `other`.
     */
  def subtract(other: RDD[T], numPartitions: Int): RDD[T] = transformation(_.subtract(other, numPartitions))
  
  /**
     * Return an RDD with the elements from `this` that are not in `other`.
     */
  def subtract(other: RDD[T], p: Partitioner)(implicit ord: Ordering[T] = noOrdering): RDD[T] = transformation(_.subtract(other, p))
  
  /**
     * Return the union of this RDD and another one. Any identical elements will appear multiple
     * times (use `.distinct()` to eliminate them).
     */
  def union(other: RDD[T]): RDD[T] = transformation(_.union(other))
  
  /**
     * Specify a ResourceProfile to use when calculating this RDD. This is only supported on
     * certain cluster managers and currently requires dynamic allocation to be enabled.
     * It will result in new executors with the resources specified being acquired to
     * calculate the RDD.
     */
  def withResources(rp: ResourceProfile): RDD[T] = transformation(_.withResources(rp))
  
  /**
     * Zips this RDD with another one, returning key-value pairs with the first element in each RDD,
     * second element in each RDD, etc. Assumes that the two RDDs have the *same number of
     * partitions* and the *same number of elements in each partition* (e.g. one was made through
     * a map on the other).
     */
  def zip[U: ClassTag](other: RDD[U]): RDD[(T, U)] = transformation(_.zip(other))
  
  /**
     * Zip this RDD's partitions with one (or more) RDD(s) and return a new RDD by
     * applying a function to the zipped partitions. Assumes that all the RDDs have the
     * *same number of partitions*, but does *not* require them to have the same number
     * of elements in each partition.
     */
  def zipPartitions[B: ClassTag, V: ClassTag](rdd2: RDD[B], preservesPartitioning: Boolean)(f: (Iterator[T], Iterator[B]) => Iterator[V]): RDD[V] = transformation(_.zipPartitions(rdd2, preservesPartitioning)(f))
  
  def zipPartitions[B: ClassTag, V: ClassTag](rdd2: RDD[B])(f: (Iterator[T], Iterator[B]) => Iterator[V]): RDD[V] = transformation(_.zipPartitions(rdd2)(f))
  
  def zipPartitions[B: ClassTag, C: ClassTag, V: ClassTag](rdd2: RDD[B], rdd3: RDD[C], preservesPartitioning: Boolean)(f: (Iterator[T], Iterator[B], Iterator[C]) => Iterator[V]): RDD[V] = transformation(_.zipPartitions(rdd2, rdd3, preservesPartitioning)(f))
  
  def zipPartitions[B: ClassTag, C: ClassTag, V: ClassTag](rdd2: RDD[B], rdd3: RDD[C])(f: (Iterator[T], Iterator[B], Iterator[C]) => Iterator[V]): RDD[V] = transformation(_.zipPartitions(rdd2, rdd3)(f))
  
  def zipPartitions[B: ClassTag, C: ClassTag, D: ClassTag, V: ClassTag](rdd2: RDD[B], rdd3: RDD[C], rdd4: RDD[D], preservesPartitioning: Boolean)(f: (Iterator[T], Iterator[B], Iterator[C], Iterator[D]) => Iterator[V]): RDD[V] = transformation(_.zipPartitions(rdd2, rdd3, rdd4, preservesPartitioning)(f))
  
  def zipPartitions[B: ClassTag, C: ClassTag, D: ClassTag, V: ClassTag](rdd2: RDD[B], rdd3: RDD[C], rdd4: RDD[D])(f: (Iterator[T], Iterator[B], Iterator[C], Iterator[D]) => Iterator[V]): RDD[V] = transformation(_.zipPartitions(rdd2, rdd3, rdd4)(f))
  
  /**
     * Zips this RDD with its element indices. The ordering is first based on the partition index
     * and then the ordering of items within each partition. So the first item in the first
     * partition gets index 0, and the last item in the last partition receives the largest index.
     *
     * This is similar to Scala's zipWithIndex but it uses Long instead of Int as the index type.
     * This method needs to trigger a spark job when this RDD contains more than one partitions.
     *
     * @note Some RDDs, such as those returned by groupBy(), do not guarantee order of
     * elements in a partition. The index assigned to each element is therefore not guaranteed,
     * and may even change if the RDD is reevaluated. If a fixed ordering is required to guarantee
     * the same index assignments, you should sort the RDD with sortByKey() or save it to a file.
     */
  def zipWithIndex: RDD[(T, Long)] = transformation(_.zipWithIndex())
  
  /**
     * Zips this RDD with generated unique Long ids. Items in the kth partition will get ids k, n+k,
     * 2*n+k, ..., where n is the number of partitions. So there may exist gaps, but this method
     * won't trigger a spark job, which is different from [[org.apache.spark.rdd.RDD#zipWithIndex]].
     *
     * @note Some RDDs, such as those returned by groupBy(), do not guarantee order of
     * elements in a partition. The unique ID assigned to each element is therefore not guaranteed,
     * and may even change if the RDD is reevaluated. If a fixed ordering is required to guarantee
     * the same index assignments, you should sort the RDD with sortByKey() or save it to a file.
     */
  def zipWithUniqueId: RDD[(T, Long)] = transformation(_.zipWithUniqueId())
  
  //===============
  
  /**
   * Methods to implement
   *
   * [[org.apache.spark.rdd.RDD.context]]
   * [[org.apache.spark.rdd.RDD.randomSplit]]
   * [[org.apache.spark.rdd.RDD.sparkContext]]
   * [[org.apache.spark.rdd.RDD.toJavaRDD]]
   */
  
  //===============
  
  /**
   * Ignored method
   *
   * [[org.apache.spark.rdd.RDD.collect]]
   * [[org.apache.spark.rdd.RDD.setName]]
   * [[org.apache.spark.rdd.RDD.toString]]
   */
}
