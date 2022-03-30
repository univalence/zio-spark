package org.apache.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.util.{CallSite, Utils}
import org.apache.spark.util.Utils.sparkInternalExclusionFunction

import zio.ZTraceElement

import scala.collection.mutable.ArrayBuffer

/**
 * The Sniffer singleton provide a backdoor to access private spark
 * function.
 */
object Sniffer {

  /** Backdoor for Dataset's showString private function. */
  def datasetShowString[T](dataset: Dataset[T], _numRows: Int, truncate: Int): String =
    dataset.showString(_numRows, truncate)

  /**
   * Backdoor for SparkSession Builder's sparkContext private function.
   */
  def sparkSessionBuilderSparkContext(builder: SparkSession.Builder, sparkContext: SparkContext): SparkSession.Builder =
    builder.sparkContext(sparkContext)

  /** Backdoor for SparkContext's setCallSite private function. */
  def sparkContextSetCallSite(sc: SparkContext, callSite: CallSite): Unit = sc.setCallSite(callSite)

  /** Backdoor for Utils's getCallSite private function. */
  def utilsGetCallSite(skipClass: String => Boolean): CallSite = getCallSite(skipClass)

  /**
   * When called inside a class in the spark package, returns the name
   * of the user code class (outside the spark package) that called into
   * Spark, as well as which Spark method they called. This is used, for
   * example, to tell users where in their code each RDD got created.
   *
   * @param skipClass
   *   Function that is used to exclude non-user-code classes.
   */
  def getCallSite(skipClass: String => Boolean): CallSite = {
    // Keep crawling up the stack trace until we find the first function not inside of the spark
    // package. We track the last (shallowest) contiguous Spark method. This might be an RDD
    // transformation, a SparkContext function (such as parallelize), or anything else that leads
    // to instantiation of an RDD. We also track the first (deepest) user method, file, and line.
    var lastSparkMethod = "<unknown>"
    var firstUserFile   = "<unknown>"
    var firstUserLine   = 0
    var insideSpark     = true
    val callStack       = new ArrayBuffer[String]() :+ "<unknown>"

    Thread.currentThread.getStackTrace.foreach { ste: StackTraceElement =>
      val zTraceElement: ZTraceElement = ZTraceElement.fromJava(ste)

      // When running under some profilers, the current stack trace might contain some bogus
      // frames. This is intended to ensure that we don't crash in these situations by
      // ignoring any frames that we can't examine.
      if (
        ste != null && ste.getMethodName != null
        && !ste.getMethodName.contains("getStackTrace")
      ) {
        if (insideSpark) {
          if (skipClass(ste.getClassName)) {
            lastSparkMethod =
              if (ste.getMethodName == "<init>") {
                // Spark method is a constructor; get its class name
                ste.getClassName.substring(ste.getClassName.lastIndexOf('.') + 1)
              } else {
                ste.getMethodName
              }
            callStack(0) = ste.toString // Put last Spark method on top of the stack trace.
          } else {
            if (ste.getFileName != null) {
              firstUserFile = ste.getFileName
              if (ste.getLineNumber >= 0) {
                firstUserLine = ste.getLineNumber
              }
            }
            callStack += ste.toString
            insideSpark = false
          }
        } else {
          callStack += ste.toString
        }
      }
    }

    val callStackDepth = System.getProperty("spark.callstack.depth", "20").toInt
    val shortForm =
      if (firstUserFile == "HiveSessionImpl.java") {
        // To be more user friendly, show a nicer string for queries submitted from the JDBC
        // server.
        "Spark JDBC Server Query"
      } else {
        s"$lastSparkMethod at $firstUserFile:$firstUserLine"
      }
    val longForm = callStack.take(callStackDepth).mkString("\n")

    CallSite(shortForm, longForm)
  }
}
