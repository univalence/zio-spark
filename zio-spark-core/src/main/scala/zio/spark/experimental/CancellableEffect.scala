package zio.spark.experimental

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Experimental

import zio.{RIO, Trace, Unsafe, ZIO}
import zio.internal.ExecutionMetrics
import zio.spark.sql.SparkSession

@Experimental
object CancellableEffect {

  private class setGroupNameExecutor(
      executor:     zio.Executor,
      sparkContext: SparkContext,
      groupName:    String
  ) extends zio.Executor {
    override def metrics(implicit unsafe: Unsafe): Option[ExecutionMetrics] = executor.metrics

    override def submit(runnable: Runnable)(implicit unsafe: Unsafe): Boolean =
      executor.submit {
        new Runnable { // Mandatory for scala 2.11
          override def run(): Unit = {
            if (!Option(sparkContext.getLocalProperty("spark.jobGroup.id")).contains(groupName))
              sparkContext.setJobGroup(groupName, "cancellable job group")

            runnable.run()
          }
        }
      }
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
  def makeItCancellable[R <: SparkSession, T](job: RIO[R, T])(implicit trace: Trace): RIO[R, T] =
    for {
      groupName <- zio.Random.nextUUID.map("cancellable-group-" + _.toString)
      sc        <- zio.spark.sql.fromSpark(_.sparkContext)
      executor  <- ZIO.executor
      x <-
        job
          .onExecutor(new setGroupNameExecutor(executor, sc, groupName))
          .disconnect
          .onInterrupt(ZIO.attempt(sc.cancelJobGroup(groupName)).ignore)
    } yield x

}
