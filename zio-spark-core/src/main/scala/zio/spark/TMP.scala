package zio.spark

import org.apache.hadoop.conf.Configuration
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.util.Utils
import org.apache.spark.{SparkConf, SparkStatusTracker, SparkContext => UnderlyingSparkContext}
import zio._
import zio.spark.rdd.RDD

import scala.reflect.ClassTag

final case class TMP(underlyingSparkContext: UnderlyingSparkContext) {

  val startTime: Long = underlyingSparkContext.startTime

  def getConf: SparkConf = underlyingSparkContext.getConf

  def resources: Map[String, ResourceInformation] = underlyingSparkContext.resources.toMap
  def jars: Seq[String] = underlyingSparkContext.jars
  def files: Seq[String] = underlyingSparkContext.files
  def archives: Seq[String] = underlyingSparkContext.archives
  def master: String = underlyingSparkContext.master
  def deployMode: String = underlyingSparkContext.deployMode
  def appName: String = underlyingSparkContext.appName

  def isLocal: Boolean = underlyingSparkContext.isLocal
  /**
   * @return true if context is stopped or in the midst of stopping.
   */
  def isStopped: Boolean = underlyingSparkContext.isStopped

  def statusTracker: SparkStatusTracker = underlyingSparkContext.statusTracker
  def uiWebUrl: Option[String] = underlyingSparkContext.uiWebUrl

  /**
   * A default Hadoop Configuration for the Hadoop code (e.g. file systems) that we reuse.
   *
   * @note As it will be reused in all Hadoop RDDs, it's better not to modify it unless you
   *       plan to set some global configurations for all Hadoop RDDs.
   */
  def hadoopConfiguration: Configuration = underlyingSparkContext.hadoopConfiguration

  val sparkUser: Task[String] = ZIO.attempt(underlyingSparkContext.sparkUser)

  /**
   * A unique identifier for the Spark application.
   * Its format depends on the scheduler implementation.
   * (i.e.
   * in case of local spark app something like 'local-1433865536131'
   * in case of YARN something like 'application_1433865536131_34483'
   * in case of MESOS something like 'driver-20170926223339-0001'
   * )
   */
  def applicationId: String = underlyingSparkContext.applicationId

  def applicationAttemptId: Option[String] = underlyingSparkContext.applicationAttemptId

  def setLogLevel(logLevel: String): Task[Unit] = ZIO.attempt(underlyingSparkContext.setLogLevel(logLevel))

  /**
   * Set a local property that affects jobs submitted from this thread, such as the Spark fair
   * scheduler pool. User-defined properties may also be set here. These properties are propagated
   * through to worker tasks and can be accessed there via
   * [[org.apache.spark.TaskContext#getLocalProperty]].
   *
   * These properties are inherited by child threads spawned from this thread. This
   * may have unexpected consequences when working with thread pools. The standard java
   * implementation of thread pools have worker threads spawn other worker threads.
   * As a result, local properties may propagate unpredictably.
   */
  def setLocalProperty(key: String, value: String): Task[Unit] = ZIO.attempt(underlyingSparkContext.setLocalProperty(key, value))

  /**
   * Get a local property set in this thread, or null if it is missing. See
   * `org.apache.spark.SparkContext.setLocalProperty`.
   */
  def getLocalProperty(key: String): Task[Option[String]] = ZIO.attempt(Option(underlyingSparkContext.getLocalProperty(key)))


  /**
   * Assigns a group ID to all the jobs started by this thread until the group ID is set to a
   * different value or cleared.
   *
   * Often, a unit of execution in an application consists of multiple Spark actions or jobs.
   * Application programmers can use this method to group all those jobs together and give a
   * group description. Once set, the Spark web UI will associate such jobs with this group.
   *
   * The application can also use `org.apache.spark.SparkContext.cancelJobGroup` to cancel all
   * running jobs in this group. For example,
   * {{{
   * // In the main thread:
   * sc.setJobGroup("some_job_to_cancel", "some job description")
   * sc.parallelize(1 to 10000, 2).map { i => Thread.sleep(10); i }.count()
   *
   * // In a separate thread:
   * sc.cancelJobGroup("some_job_to_cancel")
   * }}}
   *
   * @param interruptOnCancel If true, then job cancellation will result in `Thread.interrupt()`
   *                          being called on the job's executor threads. This is useful to help ensure that the tasks
   *                          are actually stopped in a timely manner, but is off by default due to HDFS-1208, where HDFS
   *                          may respond to Thread.interrupt() by marking nodes as dead.
   */
  def setJobGroup(groupId: String, description: String, interruptOnCancel: Boolean = false): Task[Unit] =
    ZIO.attempt(underlyingSparkContext.setJobGroup(groupId, description, interruptOnCancel))

  /** Clear the current thread's job group ID and its description. */
  def clearJobGroup(): Task[Unit] = ZIO.attempt(underlyingSparkContext.clearJobGroup())

  /** Distribute a local Scala collection to form an RDD.
   *
   * @note Parallelize acts lazily. If `seq` is a mutable collection and is altered after the call
   *       to parallelize and before the first action on the RDD, the resultant RDD will reflect the
   *       modified collection. Pass a copy of the argument to avoid this.
   * @note avoid using `parallelize(Seq())` to create an empty `RDD`. Consider `emptyRDD` for an
   *       RDD with no partitions, or `parallelize(Seq[T]())` for an RDD of `T` with empty partitions.
   * @param seq       Scala collection to distribute
   * @param numSlices number of partitions to divide the collection into
   * @return RDD representing distributed collection
   */
  def parallelize[T: ClassTag](seq: Seq[T], numSlices: Int = defaultParallelism): RDD[T] = ???

  def defaultParallelism: Int = underlyingSparkContext.defaultParallelism

  /**
   * Broadcast a read-only variable to the cluster, returning a
   * [[org.apache.spark.broadcast.Broadcast]] object for reading it in distributed functions.
   * The variable will be sent to each cluster only once.
   *
   * @param value value to broadcast to the Spark nodes
   * @return `Broadcast` object, a read-only variable cached on each machine
   */
  def broadcast[T: ClassTag](value: T): Task[Broadcast[T]] = ZIO.attempt(underlyingSparkContext.broadcast(value))
}
