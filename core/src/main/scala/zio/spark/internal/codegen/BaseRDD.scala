package zio.spark.internal.codegen

import scala.reflect._

import org.apache.spark.partial.{PartialResult, BoundedDouble}
import org.apache.spark.rdd.{RDD => UnderlyingRDD, RDDBarrier, PartitionCoalescer}
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Partition, TaskContext, Dependency, Partitioner}

import zio.Task
import zio.spark.impure.Impure
import zio.spark.impure.Impure.ImpureBox
import zio.spark.rdd.RDD
import zio.spark.rdd.RDD


abstract class BaseRDD[T](underlyingRDD: ImpureBox[UnderlyingRDD[T]]) extends Impure[UnderlyingRDD[T]](underlyingRDD) {
  import underlyingRDD._

  private implicit def arrayToSeq1[U](x: RDD[Array[U]]): RDD[Seq[U]] = x.map(_.toSeq)
  private implicit def arrayToSeq2[U](x: UnderlyingRDD[Array[U]]): UnderlyingRDD[Seq[U]] = x.map(_.toSeq)
  private implicit def lift[U](x:UnderlyingRDD[U]):RDD[U] = RDD(x)
  private implicit def escape[U](x:RDD[U]):UnderlyingRDD[U] = x.underlyingRDD.succeedNow(v => v)
  
  private implicit def iteratorConversion[T](iterator: java.util.Iterator[T]):Iterator[T] = scala.collection.JavaConverters.asScalaIteratorConverter(iterator).asScala
  
  /** Applies an action to the underlying RDD. */
  def action[U](f: UnderlyingRDD[T] => U): Task[U] = attemptBlocking(f)

  /** Applies a transformation to the underlying RDD. */
  def transformation[U](f: UnderlyingRDD[T] => UnderlyingRDD[U]): RDD[U] = succeedNow(f.andThen(x => RDD(x)))

  def barrier: RDDBarrier[T] = succeedNow(_.barrier())
  def getNumPartitions: Int = succeedNow(_.getNumPartitions)
  def id: Int = succeedNow(_.id)
  def partitioner: Option[Partitioner] = succeedNow(_.partitioner)
  def partitions: Seq[Partition] = succeedNow(_.partitions)
  def preferredLocations(split: Partition): Seq[String] = succeedNow(_.preferredLocations(split))
  def toDebugString: String = succeedNow(_.toDebugString)
  
  //===============
  
  def aggregate[U](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U)(implicit evidence$31: ClassTag[U]): Task[U] = action(_.aggregate(zeroValue)(seqOp, combOp))
  def collect[U](f: PartialFunction[T, U])(implicit evidence$30: ClassTag[U]): Task[RDD[U]] = action(_.collect(f))
  def collect: Task[Seq[T]] = action(_.collect())
  def count: Task[Long] = action(_.count())
  def countApprox(timeout: Long, confidence: Double): Task[PartialResult[BoundedDouble]] = action(_.countApprox(timeout, confidence))
  def countApproxDistinct(relativeSD: Double): Task[Long] = action(_.countApproxDistinct(relativeSD))
  def countApproxDistinct(p: Int, sp: Int): Task[Long] = action(_.countApproxDistinct(p, sp))
  def countByValue(implicit ord: Ordering[T] = null): Task[collection.Map[T, Long]] = action(_.countByValue())
  def countByValueApprox(timeout: Long, confidence: Double)(implicit ord: Ordering[T] = null): Task[PartialResult[collection.Map[T, BoundedDouble]]] = action(_.countByValueApprox(timeout, confidence))
  def first: Task[T] = action(_.first())
  def fold(zeroValue: T)(op: (T, T) => T): Task[T] = action(_.fold(zeroValue)(op))
  def foreach(f: T => Unit): Task[Unit] = action(_.foreach(f))
  def foreachPartition(f: Iterator[T] => Unit): Task[Unit] = action(_.foreachPartition(f))
  def isEmpty: Task[Boolean] = action(_.isEmpty())
  def iterator(split: Partition, context: TaskContext): Task[Iterator[T]] = action(_.iterator(split, context))
  def max(implicit ord: Ordering[T]): Task[T] = action(_.max())
  def min(implicit ord: Ordering[T]): Task[T] = action(_.min())
  def reduce(f: (T, T) => T): Task[T] = action(_.reduce(f))
  def saveAsObjectFile(path: String): Task[Unit] = action(_.saveAsObjectFile(path))
  def take(num: Int): Task[Seq[T]] = action(_.take(num))
  def takeOrdered(num: Int)(implicit ord: Ordering[T]): Task[Seq[T]] = action(_.takeOrdered(num))
  def takeSample(withReplacement: Boolean, num: Int, seed: Long): Task[Seq[T]] = action(_.takeSample(withReplacement, num, seed))
  def toLocalIterator: Task[Iterator[T]] = action(_.toLocalIterator)
  def top(num: Int)(implicit ord: Ordering[T]): Task[Seq[T]] = action(_.top(num))
  def treeAggregate[U](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U, depth: Int)(implicit evidence$32: ClassTag[U]): Task[U] = action(_.treeAggregate(zeroValue)(seqOp, combOp, depth))
  def treeReduce(f: (T, T) => T, depth: Int): Task[T] = action(_.treeReduce(f, depth))
  
  //===============
  
  def cache: Task[RDD[T]] = action(_.cache())
  def checkpoint: Task[Unit] = action(_.checkpoint())
  def dependencies: Task[Seq[Dependency[_]]] = action(_.dependencies)
  def getCheckpointFile: Task[Option[String]] = action(_.getCheckpointFile)
  def getResourceProfile: Task[ResourceProfile] = action(_.getResourceProfile())
  def getStorageLevel: Task[StorageLevel] = action(_.getStorageLevel)
  def isCheckpointed: Task[Boolean] = action(_.isCheckpointed)
  def localCheckpoint: Task[RDD[T]] = action(_.localCheckpoint())
  def name: Task[String] = action(_.name)
  def persist: Task[RDD[T]] = action(_.persist())
  def persist(newLevel: StorageLevel): Task[RDD[T]] = action(_.persist(newLevel))
  def unpersist(blocking: Boolean): Task[RDD[T]] = action(_.unpersist(blocking))
  
  //===============
  
  def cartesian[U](other: RDD[U])(implicit evidence$5: ClassTag[U]): RDD[(T, U)] = transformation(_.cartesian(other))
  def coalesce(numPartitions: Int, shuffle: Boolean, partitionCoalescer: Option[PartitionCoalescer])(implicit ord: Ordering[T] = null): RDD[T] = transformation(_.coalesce(numPartitions, shuffle, partitionCoalescer))
  def distinct: RDD[T] = transformation(_.distinct())
  def distinct(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T] = transformation(_.distinct(numPartitions))
  def filter(f: T => Boolean): RDD[T] = transformation(_.filter(f))
  def flatMap[U](f: T => TraversableOnce[U])(implicit evidence$4: ClassTag[U]): RDD[U] = transformation(_.flatMap(f))
  def glom: RDD[Seq[T]] = transformation(_.glom())
  def groupBy[K](f: T => K, p: Partitioner)(implicit kt: ClassTag[K], ord: Ordering[K] = null): RDD[(K, Iterable[T])] = transformation(_.groupBy(f, p))
  def groupBy[K](f: T => K, numPartitions: Int)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])] = transformation(_.groupBy(f, numPartitions))
  def groupBy[K](f: T => K)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])] = transformation(_.groupBy(f))
  def intersection(other: RDD[T], numPartitions: Int): RDD[T] = transformation(_.intersection(other, numPartitions))
  def intersection(other: RDD[T], partitioner: Partitioner)(implicit ord: Ordering[T] = null): RDD[T] = transformation(_.intersection(other, partitioner))
  def intersection(other: RDD[T]): RDD[T] = transformation(_.intersection(other))
  def keyBy[K](f: T => K): RDD[(K, T)] = transformation(_.keyBy(f))
  def map[U](f: T => U)(implicit evidence$3: ClassTag[U]): RDD[U] = transformation(_.map(f))
  def mapPartitions[U](f: Iterator[T] => Iterator[U], preservesPartitioning: Boolean)(implicit evidence$6: ClassTag[U]): RDD[U] = transformation(_.mapPartitions(f, preservesPartitioning))
  def mapPartitionsWithIndex[U](f: (Int, Iterator[T]) => Iterator[U], preservesPartitioning: Boolean)(implicit evidence$9: ClassTag[U]): RDD[U] = transformation(_.mapPartitionsWithIndex(f, preservesPartitioning))
  def pipe(command: Seq[String], env: collection.Map[String, String], printPipeContext: (String => Unit) => Unit, printRDDElement: (T, String => Unit) => Unit, separateWorkingDir: Boolean, bufferSize: Int, encoding: String): RDD[String] = transformation(_.pipe(command, env, printPipeContext, printRDDElement, separateWorkingDir, bufferSize, encoding))
  def pipe(command: String, env: collection.Map[String, String]): RDD[String] = transformation(_.pipe(command, env))
  def pipe(command: String): RDD[String] = transformation(_.pipe(command))
  def repartition(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T] = transformation(_.repartition(numPartitions))
  def sample(withReplacement: Boolean, fraction: Double, seed: Long): RDD[T] = transformation(_.sample(withReplacement, fraction, seed))
  def setName(_name: String): RDD[T] = transformation(_.setName(_name))
  def sortBy[K](f: T => K, ascending: Boolean, numPartitions: Int)(implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T] = transformation(_.sortBy(f, ascending, numPartitions))
  def subtract(other: RDD[T], p: Partitioner)(implicit ord: Ordering[T] = null): RDD[T] = transformation(_.subtract(other, p))
  def subtract(other: RDD[T], numPartitions: Int): RDD[T] = transformation(_.subtract(other, numPartitions))
  def subtract(other: RDD[T]): RDD[T] = transformation(_.subtract(other))
  def union(other: RDD[T]): RDD[T] = transformation(_.union(other))
  def withResources(rp: ResourceProfile): RDD[T] = transformation(_.withResources(rp))
  def zip[U](other: RDD[U])(implicit evidence$11: ClassTag[U]): RDD[(T, U)] = transformation(_.zip(other))
  def zipPartitions[B, C, D, V](rdd2: RDD[B], rdd3: RDD[C], rdd4: RDD[D])(f: (Iterator[T], Iterator[B], Iterator[C], Iterator[D]) => Iterator[V])(implicit evidence$26: ClassTag[B], evidence$27: ClassTag[C], evidence$28: ClassTag[D], evidence$29: ClassTag[V]): RDD[V] = transformation(_.zipPartitions(rdd2, rdd3, rdd4)(f))
  def zipPartitions[B, C, D, V](rdd2: RDD[B], rdd3: RDD[C], rdd4: RDD[D], preservesPartitioning: Boolean)(f: (Iterator[T], Iterator[B], Iterator[C], Iterator[D]) => Iterator[V])(implicit evidence$22: ClassTag[B], evidence$23: ClassTag[C], evidence$24: ClassTag[D], evidence$25: ClassTag[V]): RDD[V] = transformation(_.zipPartitions(rdd2, rdd3, rdd4, preservesPartitioning)(f))
  def zipPartitions[B, C, V](rdd2: RDD[B], rdd3: RDD[C])(f: (Iterator[T], Iterator[B], Iterator[C]) => Iterator[V])(implicit evidence$19: ClassTag[B], evidence$20: ClassTag[C], evidence$21: ClassTag[V]): RDD[V] = transformation(_.zipPartitions(rdd2, rdd3)(f))
  def zipPartitions[B, C, V](rdd2: RDD[B], rdd3: RDD[C], preservesPartitioning: Boolean)(f: (Iterator[T], Iterator[B], Iterator[C]) => Iterator[V])(implicit evidence$16: ClassTag[B], evidence$17: ClassTag[C], evidence$18: ClassTag[V]): RDD[V] = transformation(_.zipPartitions(rdd2, rdd3, preservesPartitioning)(f))
  def zipPartitions[B, V](rdd2: RDD[B])(f: (Iterator[T], Iterator[B]) => Iterator[V])(implicit evidence$14: ClassTag[B], evidence$15: ClassTag[V]): RDD[V] = transformation(_.zipPartitions(rdd2)(f))
  def zipPartitions[B, V](rdd2: RDD[B], preservesPartitioning: Boolean)(f: (Iterator[T], Iterator[B]) => Iterator[V])(implicit evidence$12: ClassTag[B], evidence$13: ClassTag[V]): RDD[V] = transformation(_.zipPartitions(rdd2, preservesPartitioning)(f))
  def zipWithIndex: RDD[(T, Long)] = transformation(_.zipWithIndex())
  def zipWithUniqueId: RDD[(T, Long)] = transformation(_.zipWithUniqueId())
  
  //===============
  
  /**
   * Methods to implement
   *
   * [[org.apache.spark.rdd.RDD.context]]
   * [[org.apache.spark.rdd.RDD.randomSplit]]
   * [[org.apache.spark.rdd.RDD.saveAsTextFile]]
   * [[org.apache.spark.rdd.RDD.sparkContext]]
   * [[org.apache.spark.rdd.RDD.toJavaRDD]]
   */
  
  //===============
  
  /**
   * Ignored method
   *
   * [[org.apache.spark.rdd.RDD.cleanShuffleDependencies]]
   * [[org.apache.spark.rdd.RDD.compute]]
   * [[org.apache.spark.rdd.RDD.toString]]
   */
}
