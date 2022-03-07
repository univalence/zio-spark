package zio.spark.effect

import org.apache.spark.annotation.Experimental

import zio.UIO
import zio.spark.sql.Spark

import scala.concurrent.ExecutionContext
import scala.util.Random

object CancellableEffect {

  @Experimental
  def makeItCancellable[T](job: Spark[T]): Spark[T] = {
    val newGroupName = UIO("cancellable-group-" + Random.alphanumeric.take(6).mkString)

    for {
      sc        <- zio.spark.sql.fromSpark(_.sparkContext)
      groupName <- newGroupName
      x <-
        job
          .onExecutionContext(
            new ExecutionContext {
              val global: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global

              override def execute(runnable: Runnable): Unit =
                global.execute(new Runnable {
                  override def run(): Unit = {
                    sc.setJobGroup(groupName, "cancellable job group")
                    runnable.run()
                  }
                })
              override def reportFailure(cause: Throwable): Unit = global.reportFailure(cause)
            }
          )
          .disconnect
          .onInterrupt(UIO {
            sc.cancelJobGroup(groupName)
          })
    } yield x
  }

}
