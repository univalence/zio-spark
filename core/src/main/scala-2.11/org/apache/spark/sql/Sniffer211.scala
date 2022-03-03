package org.apache.spark.sql

import org.apache.spark.sql.internal.SessionState

/**
 * The Sniffer singleton provide a backdoor to access private spark
 * function.
 */
object Sniffer211 {
  /** Backdoor for withActive private function. */
  def sessionState(sparkSession: SparkSession): SessionState = sparkSession.sessionState
}
