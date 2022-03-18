package zio.spark.experimental

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Experimental

import zio.{UIO, ZIO}
import zio.internal.ExecutionMetrics
import zio.spark.sql.{SparkSession, SRIO}

import scala.util.Random

@Experimental
object CancellableEffect {

  private class setGroupNameExecutor(
      executor:     zio.Executor,
      sparkContext: SparkContext,
      groupName:    String
  ) extends zio.Executor {
    override def unsafeMetrics: Option[ExecutionMetrics] = executor.unsafeMetrics

    override def unsafeSubmit(runnable: Runnable): Boolean =
      executor.unsafeSubmit(new Runnable {
        override def run(): Unit = {
          if (!Option(sparkContext.getLocalProperty("spark.jobGroup.id")).contains(groupName))
            sparkContext.setJobGroup(groupName, "cancellable job group")

          runnable.run()
        }
      })

    override def yieldOpCount: Int = executor.yieldOpCount
  }

  /**
   * Make a spark job cancellable by using a unique jobGroup to send a
   * cancel signal when ZIO is interrupting the fiber.
   *
   * To set the jobGroup correctly when using spark actions, the task if
   * forced to be executed on a standard global scala execution context
   * (scala.concurrent.ExecutionContext.Implicits.global), then call
   * sparkContext.setJobGroup before each statement (for now, until we
   * find a better way)
   *
   * @param job
   * @tparam T
   * @return
   */
  @Experimental
  def makeItCancellable[R, T](job: SRIO[R, T]): SRIO[R, T] =
    for {
      groupName <- UIO("cancellable-group-" + Random.alphanumeric.take(6).mkString) // FIND A SEQ GEN ?
      sc        <- zio.spark.sql.fromSpark(_.sparkContext)
      runtime   <- ZIO.runtime[SparkSession]
      x <-
        job
          .onExecutor(new setGroupNameExecutor(runtime.runtimeConfig.executor, sc, groupName))
          .disconnect
          .onInterrupt(UIO(sc.cancelJobGroup(groupName)))
    } yield x

}
