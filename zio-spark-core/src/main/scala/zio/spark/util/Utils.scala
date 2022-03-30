package zio.spark.util

import scala.util.matching.Regex

object Utils {
  val SPARK_CORE_CLASS_REGEX: Regex = """^org\.apache\.spark(\.api\.java)?(\.util)?(\.rdd)?(\.broadcast)?\.[A-Z]""".r
  val SPARK_SQL_CLASS_REGEX: Regex  = """^org\.apache\.spark\.sql.*""".r
  val ZIO_SPARK_CLASS_REGEX: Regex  = """^zio\.spark\.sql.*""".r
  val ZIO_CLASS_REGEX: Regex        = """^zio\.[a-zA-Z]""".r

  /** Filtering function for ZIO Spark callsite. */
  def zioSparkInternalExclusionFunction(className: String): Boolean = {
    val SCALA_CORE_CLASS_PREFIX = "scala"
    val isSparkClass =
      SPARK_CORE_CLASS_REGEX
        .findFirstIn(className)
        .orElse(SPARK_SQL_CLASS_REGEX.findFirstIn(className))
        .isDefined
    val isZioClass =
      ZIO_SPARK_CLASS_REGEX
        .findFirstIn(className)
        .orElse(ZIO_CLASS_REGEX.findFirstIn(className))
        .isDefined

    val isScalaClass = className.startsWith(SCALA_CORE_CLASS_PREFIX)

    isZioClass || isSparkClass || isScalaClass
  }
}
