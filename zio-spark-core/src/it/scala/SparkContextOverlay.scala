import zio.spark._
import zio.spark.rdd._
import zio._

import scala.collection.Map

/** Handmade functions for SparkContext shared for all Scala versions. */
class SparkContextOverlay(self: SparkContext) {

  // template:on

  /**
   * Returns an immutable map of RDDs that have marked themselves as persistent via cache() call.
   *
   * @note This does not necessarily mean the caching or computation was successful.
   */
  def getPersistentRDDs: Task[Map[Int, RDD[_]]] = ZIO.attempt(
    self.underlying.getPersistentRDDs.view.mapValues(rdd => RDD(rdd)).toMap
  )

  // template:off
}
