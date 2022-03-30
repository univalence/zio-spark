package zio.spark.util

import scala.util.matching.Regex

object Utils {
  val SPARK_CORE_CLASS_REGEX: Regex = """^org\.apache\.spark(\.api\.java)?(\.util)?(\.rdd)?(\.broadcast)?\.[A-Z]""".r
  val SPARK_SQL_CLASS_REGEX: Regex  = """^org\.apache\.spark\.sql.*""".r

  /** Filtering function for ZIO Spark callsite. */
  def zioSparkInternalExclusionFunction(className: String): Boolean = {
    val isSparkClass =
      SPARK_CORE_CLASS_REGEX
        .findFirstIn(className)
        .orElse(SPARK_SQL_CLASS_REGEX.findFirstIn(className))
        .isDefined
    val isZioClass   = className.startsWith("zio")
    val isScalaClass = className.startsWith("scala")

    isZioClass || isSparkClass || isScalaClass
  }
}
