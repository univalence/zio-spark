package zio.spark.effect

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Experimental

import zio.{Executor, UIO, ZIO}
import zio.internal.ExecutionMetrics
import zio.spark.sql.{Spark, SparkSession}

import scala.util.Random

object CancellableEffect {

  private def setGroupNameExecutor(
      executor: zio.Executor,
      sparkContext: SparkContext,
      groupName: String
  ): zio.Executor =
    new Executor {
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
  def makeItCancellable[T](job: Spark[T]): Spark[T] =
    for {
      groupName <- UIO("cancellable-group-" + Random.alphanumeric.take(6).mkString) // FIND A SEQ GEN ?
      sc        <- zio.spark.sql.fromSpark(_.sparkContext)
      runtime   <- ZIO.runtime[SparkSession]
      executor = setGroupNameExecutor(runtime.runtimeConfig.executor, sc, groupName)
      x <- job.onExecutor(executor).disconnect.onInterrupt(UIO(sc.cancelJobGroup(groupName)))
    } yield x

}
