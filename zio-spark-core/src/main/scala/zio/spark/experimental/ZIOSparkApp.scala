package zio.spark.experimental

import zio._

trait ZIOSparkApp extends ZIOAppVersionSpecific {
  type Environment

  val bootstrap: ZLayer[ZIOAppArgs, Any, Environment]

  def run: ZIO[Environment with ZIOAppArgs with Scope, Any, Any]

  def runtime: Runtime[Any] = Runtime.default

  final def main(args0: Array[String]): Unit = {
    implicit val trace: Trace = Trace.empty

    val newLayer =
      ZLayer.succeed(ZIOAppArgs(Chunk.fromIterable(args0))) >>> bootstrap +!+ ZLayer.environment[ZIOAppArgs]

    val workflow =
      (for {
        runtime <- ZIO.runtime[Environment with ZIOAppArgs]
        result  <- runtime.run(ZIO.scoped[Environment with ZIOAppArgs](run)).tapErrorCause(ZIO.logErrorCause(_))
      } yield result).provideLayer(newLayer.tapErrorCause(ZIO.logErrorCause(_)))

    Unsafe.unsafe { implicit unsafe =>
      runtime.unsafe
        .run {
          for {
            fiberId <- ZIO.fiberId
            fiber   <- workflow.ensuring(interruptRootFibers(fiberId)).fork
            _       <- fiber.join
          } yield ()
        }
        .getOrThrowFiberFailure()
    }
  }

  private def interruptRootFibers(fiberId: FiberId)(implicit trace: Trace): UIO[Unit] =
    for {
      roots <- Fiber.roots
      _     <- Fiber.interruptAll(roots.view.filterNot(_.id == fiberId))
    } yield ()
}
