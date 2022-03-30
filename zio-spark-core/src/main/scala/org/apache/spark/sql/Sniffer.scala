package org.apache.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.util.CallSite

import zio.{Chunk, Ref, System, Task, UIO, ZIO, ZTraceElement}

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

  /**
   * When called inside a class in the spark package, returns the name
   * of the user code class (outside the spark package) that called into
   * Spark, as well as which Spark method they called. This is used, for
   * example, to tell users where in their code each RDD got created.
   *
   * @param skipClass
   *   Function that is used to exclude non-user-code classes.
   */
  def getCallSite(skipClass: String => Boolean)(implicit trace: ZTraceElement): ZIO[System, Throwable, CallSite] = {
    // Keep crawling up the stack trace until we find the first function not inside of the spark
    // package. We track the last (shallowest) contiguous Spark method. This might be an RDD
    // transformation, a SparkContext function (such as parallelize), or anything else that leads
    // to instantiation of an RDD. We also track the first (deepest) user method, file, and line.
    case class Context(
        lastSparkMethod: String,
        firstUserFile:   String,
        firstUserLine:   Int,
        callStack:       Chunk[String],
        insideSpark:     Boolean
    ) { self =>
      def shortForm: String =
        if (firstUserFile == "HiveSessionImpl.java") {
          "Spark JDBC Server Query"
        } else {
          s"$lastSparkMethod at $firstUserFile:$firstUserLine"
        }

      def prependStackTrace(trace: String): Context = copy(callStack = self.callStack.prepended(trace))

      def appendStackTrace(trace: String): Context = copy(callStack = self.callStack.appended(trace))

      def updateLastSparkMethod(newLastSparkMethod: String): Context = copy(lastSparkMethod = newLastSparkMethod)

      def updateFirstUserFile(newFirstUserFile: String): Context = copy(firstUserFile = newFirstUserFile)

      def updateFirstUserLine(newFirstUserLine: Int): Context = copy(firstUserLine = newFirstUserLine)

      def updateInsideSpark(newInsideSpark: Boolean): Context = copy(insideSpark = newInsideSpark)

    }

    object Context {
      val default: Context =
        Context(
          lastSparkMethod = "<unknown>",
          firstUserFile   = "<unknown>",
          firstUserLine   = 0,
          callStack       = Chunk[String]("<unknown>"),
          insideSpark     = true
        )
    }

    def handleStackTraceElement(trace: ZTraceElement, ref: Ref[Context]): UIO[Unit] =
      ZTraceElement.toJava(trace) match {
        case None => ref.update(_.appendStackTrace(trace.toString))
        case Some(ste) if ste.getMethodName == null || !ste.getMethodName.contains("getStackTrace") =>
          ref.update(_.appendStackTrace(ste.toString))
        case Some(ste) =>
          for {
            context <- ref.get
            _ <-
              (context.insideSpark, skipClass(ste.getClassName)) match {
                case (false, _) => ref.update(_.appendStackTrace(ste.toString))
                case (true, false) =>
                  val lastSparkMethod =
                    if (ste.getMethodName == "<init>") {
                      ste.getClassName.substring(ste.getClassName.lastIndexOf('.') + 1)
                    } else {
                      ste.getMethodName
                    }
                  for {
                    _ <- ref.update(_.updateLastSparkMethod(lastSparkMethod))
                    _ <- ref.update(_.prependStackTrace(ste.toString))
                  } yield ()
                case (true, true) =>
                  for {
                    _ <-
                      ZIO
                        .when(ste.getFileName != null)(
                          ref.update(_.updateFirstUserFile(ste.getFileName)) *>
                            ZIO.when(ste.getLineNumber >= 0)(
                              ref.update(_.updateFirstUserLine(ste.getLineNumber))
                            )
                        )
                    _ <- ref.update(_.appendStackTrace(ste.toString))
                    _ <- ref.update(_.updateInsideSpark(false))
                  } yield ()
              }
          } yield ()
      }

    for {
      ztrace              <- Task.trace
      ref                 <- Ref.make(Context.default)
      _                   <- ztrace.stackTrace.mapZIODiscard(t => handleStackTraceElement(t, ref))
      context             <- ref.get
      maybeCallStackDepth <- System.property("spark.callstack.depth")
      callStackDepth <-
        maybeCallStackDepth match {
          case None    => UIO.succeed(20)
          case Some(v) => Task.attempt(v.toInt).orElse(UIO.succeed(20))
        }
      shortForm = context.shortForm
      longForm  = context.callStack.take(callStackDepth).mkString("\n")
    } yield CallSite(shortForm, longForm)
  }
}
