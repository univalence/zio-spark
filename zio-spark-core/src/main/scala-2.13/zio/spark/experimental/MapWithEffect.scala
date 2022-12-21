package zio.spark.experimental

import zio.{IO, Runtime, Trace, Unsafe}
import zio.spark.experimental.NewType.{Ratio, Weight}
import zio.spark.rdd.RDD

import scala.util.Either

object MapWithEffect {

  @SuppressWarnings(Array("scalafix:DisableSyntax.defaultArgs"))
  def apply[E1, E2 >: E1, A](rdd: RDD[IO[E1, A]])(
      onRejected: E2,
      maxErrorRatio: Ratio = Ratio.p05,
      decayScale: Weight = Weight(1000L)
  )(implicit trace: Trace): RDD[Either[E2, A]] = rdd.mapZIO(identity, _ => onRejected, maxErrorRatio, decayScale)

  implicit class RDDOps[T](private val rdd: RDD[T]) extends AnyVal {
    @SuppressWarnings(Array("scalafix:DisableSyntax.defaultArgs"))
    def mapZIO[E, B](
        effect: T => IO[E, B],
        onRejection: T => E,
        maxErrorRatio: Ratio = Ratio.p05,
        decayScale: Weight = Weight(1000L)
        // maxStack: Int = 16
    )(implicit trace: Trace): RDD[Either[E, B]] =
      rdd.mapPartitions { it: Iterator[T] =>
        type EE = Option[E]

        val createCircuit: CircuitTap[EE, EE] =
          Unsafe.unsafe { implicit u =>
            Runtime.default.unsafe
              .run(CircuitTap.make[EE, EE](maxErrorRatio, _ => true, None, decayScale))
              .getOrThrowFiberFailure()
          }

        it.map { x =>
          Unsafe.unsafe { implicit u =>
            val io = createCircuit(effect(x).asSomeError).mapError(_.getOrElse(onRejection(x))).either
            Runtime.default.unsafe.run(io).getOrThrowFiberFailure()
          }
        }
      }
  }
}
