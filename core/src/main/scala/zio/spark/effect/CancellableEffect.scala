package zio.spark.effect

import zio.UIO
import zio.spark.sql.Spark

import scala.concurrent.ExecutionContext

object CancellableEffect {

  // cancellable prototype
  def makeItCancellable[T](job: Spark[T]): Spark[T] =
    zio.spark.sql.fromSpark(_.sparkContext) flatMap { sc =>
      val name = "totoJobGroup"

      job
        .onExecutionContext(new ExecutionContext {

          val global: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global

          override def execute(runnable: Runnable): Unit =
            global.execute(new Runnable {
              override def run(): Unit = {
                sc.setJobGroup(name, "cancellable job group")
                runnable.run()
              }
            })

          override def reportFailure(cause: Throwable): Unit = global.reportFailure(cause)
        })
        .ensuring(UIO {
          println("trying cancel")
          sc.cancelJobGroup(name)
        })
    }

}
