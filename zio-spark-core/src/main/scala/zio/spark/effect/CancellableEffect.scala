package zio.spark.effect

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Experimental

import zio.UIO
import zio.spark.sql.Spark

import scala.concurrent.ExecutionContext
import scala.util.Random

object CancellableEffect {

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
  def makeItCancellable[T](job: Spark[T]): Spark[T] = {
    val newGroupName = UIO("cancellable-group-" + Random.alphanumeric.take(6).mkString)

    zio.spark.sql
      .fromSpark(_.sparkContext)
      .flatMap(sc =>
        newGroupName.flatMap(groupName =>
          job
            .onExecutionContext(executorContext(sc, groupName))
            .disconnect // you need to disconnect first, before adding onInterrupt
            .onInterrupt(UIO(sc.cancelJobGroup(groupName)))
        )
      )
  }

  //TODO : BUILD EXECUTION CONTEXT FROM THE EXISTING ZIO CONTEXT (zio.Runtime.config ...)
  private def executorContext(sparkContext: SparkContext, groupName: String): ExecutionContext =
    new ExecutionContext {
      val global: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global

      override def execute(runnable: Runnable): Unit =
        global.execute(new Runnable {
          override def run(): Unit = {
            if (!Option(sparkContext.getLocalProperty("spark.jobGroup.id")).contains(groupName))
              sparkContext.setJobGroup(groupName, "cancellable job group")

            runnable.run()
          }
        })

      override def reportFailure(cause: Throwable): Unit = global.reportFailure(cause)
    }
}
