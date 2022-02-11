package org.apache.spark.sql

/**
 * The Sniffer singleton provide a backdoor to access private spark
 * function.
 */
object Sniffer {

  /** Backdoor for showString private function. */
  def datasetShowString[T](
      dataset: Dataset[T],
      _numRows: Int,
      truncate: Int,
      vertical: Boolean
  ): String = dataset.showString(_numRows, truncate, vertical)
}
