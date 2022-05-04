package zio.spark.experimental

import zio.{IO, Trace}
import zio.spark.experimental.NewType._
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
        decayScale: Weight = Weight(1000L),
        maxStack: Int = 16
    )(implicit trace: Trace): RDD[Either[E, B]] =
      rdd.mapPartitions { it: Iterator[T] =>
        type EE = Option[E]

        val runtime: zio.Runtime[Any] = zio.Runtime.default

        val createCircuit: CircuitTap[EE, EE] =
          runtime.unsafeRun(CircuitTap.make[EE, EE](maxErrorRatio, _ => true, None, decayScale))

        it.map { x =>
          val io = createCircuit(effect(x).asSomeError).mapError(_.getOrElse(onRejection(x))).either
          runtime.unsafeRunFast(io, maxStack)
        }
      }
  }
}
