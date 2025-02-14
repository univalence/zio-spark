/**
 * /!\ Warning /!\
 *
 * This file is generated using zio-spark-codegen, you should not edit
 * this file directly.
 */

package zio.spark.rdd

import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.spark.{Dependency, Partition, Partitioner, TaskContext}
import org.apache.spark.partial.{BoundedDouble, PartialResult}
import org.apache.spark.rdd.{PartitionCoalescer, RDD => UnderlyingRDD, RDDBarrier}
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.storage.StorageLevel

import zio._

import scala.collection.Map
import scala.io.Codec
import scala.reflect._

@SuppressWarnings(Array("scalafix:DisableSyntax.defaultArgs", "scalafix:DisableSyntax.null"))
final case class RDD[T](underlying: UnderlyingRDD[T]) { self =>
  // scalafix:off
  implicit private def lift[U](x: UnderlyingRDD[U]): RDD[U]                              = RDD(x)
  implicit private def arrayToSeq2[U](x: UnderlyingRDD[Array[U]]): UnderlyingRDD[Seq[U]] = x.map(_.toIndexedSeq)
  @inline private def noOrdering[U]: Ordering[U]                                         = null
  // scalafix:on

  /** Applies an action to the underlying RDD. */
  def action[U](f: UnderlyingRDD[T] => U)(implicit trace: Trace): Task[U] = ZIO.attempt(get(f))

  /** Applies a transformation to the underlying RDD. */
  def transformation[TNew](f: UnderlyingRDD[T] => UnderlyingRDD[TNew]): RDD[TNew] = RDD(f(underlying))

  /** Applies an action to the underlying RDD. */
  def get[U](f: UnderlyingRDD[T] => U): U = f(underlying)

  // Generated functions coming from spark

  def getNumPartitions: Int = get(_.getNumPartitions)

  def partitions: Seq[Partition] = get(_.partitions.toSeq)

  def preferredLocations(split: Partition): Seq[String] = get(_.preferredLocations(split))

  def toDebugString: String = get(_.toDebugString)

  // ===============

  def aggregate[U: ClassTag](zeroValue: => U)(seqOp: (U, T) => U, combOp: (U, U) => U)(implicit trace: Trace): Task[U] =
    action(_.aggregate[U](zeroValue)(seqOp, combOp))

  def collect(implicit trace: Trace): Task[Seq[T]] = action(_.collect().toSeq)

  def count(implicit trace: Trace): Task[Long] = action(_.count())

  def countApprox(timeout: => Long, confidence: => Double = 0.95)(implicit
      trace: Trace
  ): Task[PartialResult[BoundedDouble]] = action(_.countApprox(timeout, confidence))

  def countApproxDistinct(p: => Int, sp: => Int)(implicit trace: Trace): Task[Long] =
    action(_.countApproxDistinct(p, sp))

  def countApproxDistinct(relativeSD: => Double = 0.05)(implicit trace: Trace): Task[Long] =
    action(_.countApproxDistinct(relativeSD))

  def countByValue(implicit ord: Ordering[T] = noOrdering, trace: Trace): Task[Map[T, Long]] = action(_.countByValue())

  def countByValueApprox(timeout: => Long, confidence: => Double = 0.95)(implicit
      ord: Ordering[T] = noOrdering,
      trace: Trace
  ): Task[PartialResult[Map[T, BoundedDouble]]] = action(_.countByValueApprox(timeout, confidence))

  def first(implicit trace: Trace): Task[T] = action(_.first())

  def fold(zeroValue: => T)(op: (T, T) => T)(implicit trace: Trace): Task[T] = action(_.fold(zeroValue)(op))

  def foreach(f: T => Unit)(implicit trace: Trace): Task[Unit] = action(_.foreach(f))

  def foreachPartition(f: Iterator[T] => Unit)(implicit trace: Trace): Task[Unit] = action(_.foreachPartition(f))

  def isEmpty(implicit trace: Trace): Task[Boolean] = action(_.isEmpty())

  def iterator(split: => Partition, context: => TaskContext)(implicit trace: Trace): Task[Iterator[T]] =
    action(_.iterator(split, context))

  def max(implicit ord: Ordering[T], trace: Trace): Task[T] = action(_.max())

  def min(implicit ord: Ordering[T], trace: Trace): Task[T] = action(_.min())

  def reduce(f: (T, T) => T)(implicit trace: Trace): Task[T] = action(_.reduce(f))

  def saveAsObjectFile(path: => String)(implicit trace: Trace): Task[Unit] = action(_.saveAsObjectFile(path))

  def saveAsTextFile(path: => String)(implicit trace: Trace): Task[Unit] = action(_.saveAsTextFile(path))

  def saveAsTextFile(path: => String, codec: => Class[_ <: CompressionCodec])(implicit trace: Trace): Task[Unit] =
    action(_.saveAsTextFile(path, codec))

  def take(num: => Int)(implicit trace: Trace): Task[Seq[T]] = action(_.take(num).toSeq)

  def takeOrdered(num: => Int)(implicit ord: Ordering[T], trace: Trace): Task[Seq[T]] = action(_.takeOrdered(num).toSeq)

  def takeSample(withReplacement: => Boolean, num: => Int, seed: Long)(implicit trace: Trace): Task[Seq[T]] =
    action(_.takeSample(withReplacement, num, seed).toSeq)

  def toLocalIterator(implicit trace: Trace): Task[Iterator[T]] = action(_.toLocalIterator)

  def top(num: => Int)(implicit ord: Ordering[T], trace: Trace): Task[Seq[T]] = action(_.top(num).toSeq)

  def treeAggregate[U: ClassTag](zeroValue: => U)(seqOp: (U, T) => U, combOp: (U, U) => U, depth: => Int = 2)(implicit
      trace: Trace
  ): Task[U] = action(_.treeAggregate[U](zeroValue)(seqOp, combOp, depth))

  def treeAggregate[U: ClassTag](
      zeroValue: => U,
      seqOp: (U, T) => U,
      combOp: (U, U) => U,
      depth: => Int,
      finalAggregateOnExecutor: => Boolean
  )(implicit trace: Trace): Task[U] =
    action(_.treeAggregate[U](zeroValue, seqOp, combOp, depth, finalAggregateOnExecutor))

  def treeReduce(f: (T, T) => T, depth: => Int = 2)(implicit trace: Trace): Task[T] = action(_.treeReduce(f, depth))

  // ===============

  def barrier(implicit trace: Trace): Task[RDDBarrier[T]] = action(_.barrier())

  def cache(implicit trace: Trace): Task[RDD[T]] = action(_.cache())

  def checkpoint(implicit trace: Trace): Task[Unit] = action(_.checkpoint())

  def dependencies(implicit trace: Trace): Task[Seq[Dependency[_]]] = action(_.dependencies)

  def getCheckpointFile(implicit trace: Trace): Task[Option[String]] = action(_.getCheckpointFile)

  def getResourceProfile(implicit trace: Trace): Task[ResourceProfile] = action(_.getResourceProfile())

  def getStorageLevel(implicit trace: Trace): Task[StorageLevel] = action(_.getStorageLevel)

  def isCheckpointed(implicit trace: Trace): Task[Boolean] = action(_.isCheckpointed)

  def localCheckpoint(implicit trace: Trace): Task[RDD[T]] = action(_.localCheckpoint())

  def persist(newLevel: => StorageLevel)(implicit trace: Trace): Task[RDD[T]] = action(_.persist(newLevel))

  def persist(implicit trace: Trace): Task[RDD[T]] = action(_.persist())

  def unpersist(blocking: => Boolean = false)(implicit trace: Trace): Task[RDD[T]] = action(_.unpersist(blocking))

  // ===============

  def ++(other: RDD[T]): RDD[T] = transformation(_.++(other.underlying))

  def cartesian[U: ClassTag](other: RDD[U]): RDD[(T, U)] = transformation(_.cartesian[U](other.underlying))

  def coalesce(
      numPartitions: Int,
      shuffle: Boolean = false,
      partitionCoalescer: Option[PartitionCoalescer] = Option.empty
  )(implicit ord: Ordering[T] = noOrdering): RDD[T] =
    transformation(_.coalesce(numPartitions, shuffle, partitionCoalescer))

  def distinct(numPartitions: Int)(implicit ord: Ordering[T] = noOrdering): RDD[T] =
    transformation(_.distinct(numPartitions))

  def distinct: RDD[T] = transformation(_.distinct())

  def filter(f: T => Boolean): RDD[T] = transformation(_.filter(f))

  def flatMap[U: ClassTag](f: T => IterableOnce[U]): RDD[U] = transformation(_.flatMap[U](f))

  def glom: RDD[Seq[T]] = transformation(_.glom().map(_.toSeq))

  def intersection(other: RDD[T]): RDD[T] = transformation(_.intersection(other.underlying))

  def intersection(other: RDD[T], partitioner: Partitioner)(implicit ord: Ordering[T] = noOrdering): RDD[T] =
    transformation(_.intersection(other.underlying, partitioner))

  def intersection(other: RDD[T], numPartitions: Int): RDD[T] =
    transformation(_.intersection(other.underlying, numPartitions))

  def keyBy[K](f: T => K): RDD[(K, T)] = transformation(_.keyBy[K](f))

  def map[U: ClassTag](f: T => U): RDD[U] = transformation(_.map[U](f))

  def mapPartitions[U: ClassTag](f: Iterator[T] => Iterator[U], preservesPartitioning: Boolean = false): RDD[U] =
    transformation(_.mapPartitions[U](f, preservesPartitioning))

  def mapPartitionsWithIndex[U: ClassTag](
      f: (Int, Iterator[T]) => Iterator[U],
      preservesPartitioning: Boolean = false
  ): RDD[U] = transformation(_.mapPartitionsWithIndex[U](f, preservesPartitioning))

  def pipe(command: String): RDD[String] = transformation(_.pipe(command))

  def pipe(command: String, env: Map[String, String]): RDD[String] = transformation(_.pipe(command, env))

  def pipe(
      command: Seq[String],
      env: Map[String, String] = Map(),
      printPipeContext: (String => Unit) => Unit = null,
      printRDDElement: (T, String => Unit) => Unit = null,
      separateWorkingDir: Boolean = false,
      bufferSize: Int = 8192,
      encoding: String = Codec.defaultCharsetCodec.name
  ): RDD[String] =
    transformation(_.pipe(command, env, printPipeContext, printRDDElement, separateWorkingDir, bufferSize, encoding))

  def repartition(numPartitions: Int)(implicit ord: Ordering[T] = noOrdering): RDD[T] =
    transformation(_.repartition(numPartitions))

  def sample(withReplacement: Boolean, fraction: Double, seed: Long): RDD[T] =
    transformation(_.sample(withReplacement, fraction, seed))

  def sortBy[K](f: (T) => K, ascending: Boolean = true, numPartitions: Int = this.partitions.length)(implicit
      ord: Ordering[K],
      ctag: ClassTag[K]
  ): RDD[T] = transformation(_.sortBy[K](f, ascending, numPartitions))

  def subtract(other: RDD[T]): RDD[T] = transformation(_.subtract(other.underlying))

  def subtract(other: RDD[T], numPartitions: Int): RDD[T] = transformation(_.subtract(other.underlying, numPartitions))

  def subtract(other: RDD[T], p: Partitioner)(implicit ord: Ordering[T] = noOrdering): RDD[T] =
    transformation(_.subtract(other.underlying, p))

  def union(other: RDD[T]): RDD[T] = transformation(_.union(other.underlying))

  def withResources(rp: ResourceProfile): RDD[T] = transformation(_.withResources(rp))

  def zip[U: ClassTag](other: RDD[U]): RDD[(T, U)] = transformation(_.zip[U](other.underlying))

  def zipPartitions[B: ClassTag, V: ClassTag](rdd2: RDD[B], preservesPartitioning: Boolean)(
      f: (Iterator[T], Iterator[B]) => Iterator[V]
  ): RDD[V] = transformation(_.zipPartitions[B, V](rdd2.underlying, preservesPartitioning)(f))

  def zipPartitions[B: ClassTag, V: ClassTag](rdd2: RDD[B])(f: (Iterator[T], Iterator[B]) => Iterator[V]): RDD[V] =
    transformation(_.zipPartitions[B, V](rdd2.underlying)(f))

  def zipPartitions[B: ClassTag, C: ClassTag, V: ClassTag](rdd2: RDD[B], rdd3: RDD[C], preservesPartitioning: Boolean)(
      f: (Iterator[T], Iterator[B], Iterator[C]) => Iterator[V]
  ): RDD[V] = transformation(_.zipPartitions[B, C, V](rdd2.underlying, rdd3.underlying, preservesPartitioning)(f))

  def zipPartitions[B: ClassTag, C: ClassTag, V: ClassTag](rdd2: RDD[B], rdd3: RDD[C])(
      f: (Iterator[T], Iterator[B], Iterator[C]) => Iterator[V]
  ): RDD[V] = transformation(_.zipPartitions[B, C, V](rdd2.underlying, rdd3.underlying)(f))

  def zipPartitions[B: ClassTag, C: ClassTag, D: ClassTag, V: ClassTag](
      rdd2: RDD[B],
      rdd3: RDD[C],
      rdd4: RDD[D],
      preservesPartitioning: Boolean
  )(f: (Iterator[T], Iterator[B], Iterator[C], Iterator[D]) => Iterator[V]): RDD[V] =
    transformation(
      _.zipPartitions[B, C, D, V](rdd2.underlying, rdd3.underlying, rdd4.underlying, preservesPartitioning)(f)
    )

  def zipPartitions[B: ClassTag, C: ClassTag, D: ClassTag, V: ClassTag](rdd2: RDD[B], rdd3: RDD[C], rdd4: RDD[D])(
      f: (Iterator[T], Iterator[B], Iterator[C], Iterator[D]) => Iterator[V]
  ): RDD[V] = transformation(_.zipPartitions[B, C, D, V](rdd2.underlying, rdd3.underlying, rdd4.underlying)(f))

  def zipWithIndex: RDD[(T, Long)] = transformation(_.zipWithIndex())

  def zipWithUniqueId: RDD[(T, Long)] = transformation(_.zipWithUniqueId())

  // ===============

  // Methods that need to be implemented
  //
  // [[org.apache.spark.rdd.RDD.context]]
  // [[org.apache.spark.rdd.RDD.sparkContext]]

  // ===============

  // Ignored methods
  //
  // [[org.apache.spark.rdd.RDD.collect]]
  // [[org.apache.spark.rdd.RDD.groupBy]]
  // [[org.apache.spark.rdd.RDD.setName]]
  // [[org.apache.spark.rdd.RDD.toJavaRDD]]
  // [[org.apache.spark.rdd.RDD.toString]]
}
