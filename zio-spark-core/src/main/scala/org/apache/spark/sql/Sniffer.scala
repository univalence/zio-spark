package org.apache.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.util.CallSite

import zio._

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

  def getCallSite(
      skipClass: String => Boolean
  )(implicit trace: ZTraceElement): ZIO[System with Console, Throwable, CallSite] = {
    case class Location(
        firstUserMethod: String,
        firstUserFile:   String,
        firstUserLine:   Int
    )

    object Location {
      def fromStackTraceElement(stackTraceElement: StackTraceElement): Location =
        Location(
          firstUserMethod = stackTraceElement.getMethodName,
          firstUserFile   = stackTraceElement.getFileName,
          firstUserLine   = stackTraceElement.getLineNumber
        )
    }

    case class Context(
        errorLocation: Option[Location],
        callStack:     Chunk[String]
    ) { self =>
      def shortForm: String =
        errorLocation.fold("Unknown method on user code base") { location =>
          if (location.firstUserFile == "HiveSessionImpl.java") {
            "Spark JDBC Server Query"
          } else {
            s"${location.firstUserMethod} at ${location.firstUserFile}:${location.firstUserLine}"
          }
        }

      def appendStackTrace(trace: String): Context = copy(callStack = self.callStack.appended(trace))
    }

    def handleStackTraceElement(trace: ZTraceElement, ref: Ref[Context]): RIO[Console, Unit] =
      ZTraceElement.toJava(trace) match {
        case Some(ste) if ste.getMethodName != null && !ste.getMethodName.contains("getStackTrace") =>
          for {
            context <- ref.get
            _ <-
              for {
                _ <-
                  ZIO.when(!skipClass(ste.getClassName) && context.errorLocation.isEmpty)(
                    ref.update(ctx => ctx.copy(errorLocation = Some(Location.fromStackTraceElement(ste))))
                  )
                _ <- ref.update(_.appendStackTrace(ste.toString))
              } yield ()
          } yield ()
        case _ => UIO.unit
      }

    for {
      ztrace              <- Task.trace
      _                   <- ztrace.toJava.mapZIO(a => Console.printLine(a.toString))
      ref                 <- Ref.make(Context(errorLocation = None, callStack = Chunk.empty))
      _                   <- ztrace.stackTrace.mapZIODiscard(t => handleStackTraceElement(t, ref))
      context             <- ref.get
      maybeCallStackDepth <- System.property("spark.callstack.depth")
      callStackDepth <-
        maybeCallStackDepth match {
          case None    => UIO.succeed(20)
          case Some(v) => Task.attempt(v.toInt).orElseSucceed(20)
        }
      shortForm = context.shortForm
      longForm  = context.callStack.take(callStackDepth).mkString("\n")
    } yield CallSite(shortForm, longForm)
  }
}
