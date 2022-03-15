package org.apache.spark.sql

/**
 * The Sniffer singleton provide a backdoor to access private spark
 * function.
 */
object Sniffer212 {

  /** Backdoor for withActive private function. */
  def sparkSessionWithActive[T](sparkSession: SparkSession, block: => T): T = sparkSession.withActive(block)
}
