package zio.spark.effect

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Experimental

import zio.UIO
import zio.spark.sql.Spark

import scala.concurrent.ExecutionContext
import scala.util.Random

object CancellableEffect {

  @Experimental
  def makeItCancellable[T](job: Spark[T]): Spark[T] = {
    val newGroupName = UIO("cancellable-group-" + Random.alphanumeric.take(6).mkString)

    zio.spark.sql
      .fromSpark(_.sparkContext)
      .flatMap(sc =>
        newGroupName.flatMap(groupName =>
          job
            .onExecutionContext(executorContext(sc, groupName))
            .disconnect
            .onInterrupt(UIO(sc.cancelJobGroup(groupName)))
        )
      )
  }

  private def executorContext(sparkContext: SparkContext, groupName: String): ExecutionContext =
    new ExecutionContext {
      val global: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global

      override def execute(runnable: Runnable): Unit =
        global.execute(() => {
          sparkContext.setJobGroup(groupName, "cancellable job group")
          runnable.run()
        })

      override def reportFailure(cause: Throwable): Unit = global.reportFailure(cause)
    }
}
