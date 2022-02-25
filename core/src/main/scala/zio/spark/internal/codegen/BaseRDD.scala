package zio.spark.internal.codegen


import scala.reflect._

import scala.io.Codec

import org.apache.spark.partial.{PartialResult, BoundedDouble}
import org.apache.spark.rdd.{RDD => UnderlyingRDD, RDDBarrier, PartitionCoalescer}
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Partition, TaskContext, Dependency, Partitioner}
import org.apache.spark.util.Utils
import org.apache.hadoop.io.compress.CompressionCodec

import zio.Task
import zio.spark.impure.Impure
import zio.spark.impure.Impure.ImpureBox
import zio.spark.rdd.RDD

import scala.collection.Map


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
  def partitions: Seq[Partition] = succeedNow(_.partitions)
  def preferredLocations(split: Partition): Seq[String] = succeedNow(_.preferredLocations(split))
  def toDebugString: String = succeedNow(_.toDebugString)
  
  //===============
  
  def aggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): Task[U] = action(_.aggregate(zeroValue)(seqOp, combOp))
  def collect: Task[Seq[T]] = action(_.collect())
  def count: Task[Long] = action(_.count())
  def countApprox(timeout: Long, confidence: Double = 0.95): Task[PartialResult[BoundedDouble]] = action(_.countApprox(timeout, confidence))
  def countApproxDistinct(p: Int, sp: Int): Task[Long] = action(_.countApproxDistinct(p, sp))
  def countApproxDistinct(relativeSD: Double = 0.05): Task[Long] = action(_.countApproxDistinct(relativeSD))
  def countByValue(implicit ord: Ordering[T] = null): Task[Map[T, Long]] = action(_.countByValue())
  def countByValueApprox(timeout: Long, confidence: Double = 0.95)(implicit ord: Ordering[T] = null): Task[PartialResult[Map[T, BoundedDouble]]] = action(_.countByValueApprox(timeout, confidence))
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
  def saveAsTextFile(path: String): Task[Unit] = action(_.saveAsTextFile(path))
  def saveAsTextFile(path: String, codec: Class[_ <: CompressionCodec]): Task[Unit] = action(_.saveAsTextFile(path, codec))
  def take(num: Int): Task[Seq[T]] = action(_.take(num))
  def takeOrdered(num: Int)(implicit ord: Ordering[T]): Task[Seq[T]] = action(_.takeOrdered(num))
  def takeSample(withReplacement: Boolean, num: Int, seed: Long): Task[Seq[T]] = action(_.takeSample(withReplacement, num, seed))
  def toLocalIterator: Task[Iterator[T]] = action(_.toLocalIterator)
  def top(num: Int)(implicit ord: Ordering[T]): Task[Seq[T]] = action(_.top(num))
  def treeAggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U, depth: Int = 2): Task[U] = action(_.treeAggregate(zeroValue)(seqOp, combOp, depth))
  def treeReduce(f: (T, T) => T, depth: Int = 2): Task[T] = action(_.treeReduce(f, depth))
  
  //===============
  
  def cache: Task[RDD[T]] = action(_.cache())
  def checkpoint: Task[Unit] = action(_.checkpoint())
  def dependencies: Task[Seq[Dependency[_]]] = action(_.dependencies)
  def getCheckpointFile: Task[Option[String]] = action(_.getCheckpointFile)
  def getResourceProfile: Task[ResourceProfile] = action(_.getResourceProfile())
  def getStorageLevel: Task[StorageLevel] = action(_.getStorageLevel)
  def isCheckpointed: Task[Boolean] = action(_.isCheckpointed)
  def localCheckpoint: Task[RDD[T]] = action(_.localCheckpoint())
  def persist(newLevel: StorageLevel): Task[RDD[T]] = action(_.persist(newLevel))
  def persist: Task[RDD[T]] = action(_.persist())
  def unpersist(blocking: Boolean = false): Task[RDD[T]] = action(_.unpersist(blocking))
  
  //===============
  
  def ++(other: RDD[T]): RDD[T] = transformation(_.++(other))
  def cartesian[U: ClassTag](other: RDD[U]): RDD[(T, U)] = transformation(_.cartesian(other))
  def coalesce(numPartitions: Int, shuffle: Boolean = false, partitionCoalescer: Option[PartitionCoalescer] = Option.empty)(implicit ord: Ordering[T] = null): RDD[T] = transformation(_.coalesce(numPartitions, shuffle, partitionCoalescer))
  def distinct(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T] = transformation(_.distinct(numPartitions))
  def distinct: RDD[T] = transformation(_.distinct())
  def filter(f: T => Boolean): RDD[T] = transformation(_.filter(f))
  def flatMap[U: ClassTag](f: T => TraversableOnce[U]): RDD[U] = transformation(_.flatMap(f))
  def glom: RDD[Seq[T]] = transformation(_.glom())
  def groupBy[K](f: T => K)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])] = transformation(_.groupBy(f))
  def groupBy[K](f: T => K, numPartitions: Int)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])] = transformation(_.groupBy(f, numPartitions))
  def groupBy[K](f: T => K, p: Partitioner)(implicit kt: ClassTag[K], ord: Ordering[K] = null): RDD[(K, Iterable[T])] = transformation(_.groupBy(f, p))
  def intersection(other: RDD[T]): RDD[T] = transformation(_.intersection(other))
  def intersection(other: RDD[T], partitioner: Partitioner)(implicit ord: Ordering[T] = null): RDD[T] = transformation(_.intersection(other, partitioner))
  def intersection(other: RDD[T], numPartitions: Int): RDD[T] = transformation(_.intersection(other, numPartitions))
  def keyBy[K](f: T => K): RDD[(K, T)] = transformation(_.keyBy(f))
  def map[U: ClassTag](f: T => U): RDD[U] = transformation(_.map(f))
  def mapPartitions[U: ClassTag](f: Iterator[T] => Iterator[U], preservesPartitioning: Boolean = false): RDD[U] = transformation(_.mapPartitions(f, preservesPartitioning))
  def mapPartitionsWithIndex[U: ClassTag](f: (Int, Iterator[T]) => Iterator[U], preservesPartitioning: Boolean = false): RDD[U] = transformation(_.mapPartitionsWithIndex(f, preservesPartitioning))
  def pipe(command: String): RDD[String] = transformation(_.pipe(command))
  def pipe(command: String, env: Map[String, String]): RDD[String] = transformation(_.pipe(command, env))
  def pipe(command: Seq[String], env: Map[String, String] = Map(), printPipeContext: (String => Unit) => Unit = null, printRDDElement: (T, String => Unit) => Unit = null, separateWorkingDir: Boolean = false, bufferSize: Int = 8192, encoding: String = Codec.defaultCharsetCodec.name): RDD[String] = transformation(_.pipe(command, env, printPipeContext, printRDDElement, separateWorkingDir, bufferSize, encoding))
  def repartition(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T] = transformation(_.repartition(numPartitions))
  def sample(withReplacement: Boolean, fraction: Double, seed: Long): RDD[T] = transformation(_.sample(withReplacement, fraction, seed))
  def sortBy[K](f: (T) => K, ascending: Boolean = true, numPartitions: Int = this.partitions.length)(implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T] = transformation(_.sortBy(f, ascending, numPartitions))
  def subtract(other: RDD[T]): RDD[T] = transformation(_.subtract(other))
  def subtract(other: RDD[T], numPartitions: Int): RDD[T] = transformation(_.subtract(other, numPartitions))
  def subtract(other: RDD[T], p: Partitioner)(implicit ord: Ordering[T] = null): RDD[T] = transformation(_.subtract(other, p))
  def union(other: RDD[T]): RDD[T] = transformation(_.union(other))
  def withResources(rp: ResourceProfile): RDD[T] = transformation(_.withResources(rp))
  def zip[U: ClassTag](other: RDD[U]): RDD[(T, U)] = transformation(_.zip(other))
  def zipPartitions[B: ClassTag, V: ClassTag](rdd2: RDD[B], preservesPartitioning: Boolean)(f: (Iterator[T], Iterator[B]) => Iterator[V]): RDD[V] = transformation(_.zipPartitions(rdd2, preservesPartitioning)(f))
  def zipPartitions[B: ClassTag, V: ClassTag](rdd2: RDD[B])(f: (Iterator[T], Iterator[B]) => Iterator[V]): RDD[V] = transformation(_.zipPartitions(rdd2)(f))
  def zipPartitions[B: ClassTag, C: ClassTag, V: ClassTag](rdd2: RDD[B], rdd3: RDD[C], preservesPartitioning: Boolean)(f: (Iterator[T], Iterator[B], Iterator[C]) => Iterator[V]): RDD[V] = transformation(_.zipPartitions(rdd2, rdd3, preservesPartitioning)(f))
  def zipPartitions[B: ClassTag, C: ClassTag, V: ClassTag](rdd2: RDD[B], rdd3: RDD[C])(f: (Iterator[T], Iterator[B], Iterator[C]) => Iterator[V]): RDD[V] = transformation(_.zipPartitions(rdd2, rdd3)(f))
  def zipPartitions[B: ClassTag, C: ClassTag, D: ClassTag, V: ClassTag](rdd2: RDD[B], rdd3: RDD[C], rdd4: RDD[D], preservesPartitioning: Boolean)(f: (Iterator[T], Iterator[B], Iterator[C], Iterator[D]) => Iterator[V]): RDD[V] = transformation(_.zipPartitions(rdd2, rdd3, rdd4, preservesPartitioning)(f))
  def zipPartitions[B: ClassTag, C: ClassTag, D: ClassTag, V: ClassTag](rdd2: RDD[B], rdd3: RDD[C], rdd4: RDD[D])(f: (Iterator[T], Iterator[B], Iterator[C], Iterator[D]) => Iterator[V]): RDD[V] = transformation(_.zipPartitions(rdd2, rdd3, rdd4)(f))
  def zipWithIndex: RDD[(T, Long)] = transformation(_.zipWithIndex())
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
