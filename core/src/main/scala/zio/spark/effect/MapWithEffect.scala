package zio.spark.effect

import zio.{IO, UIO, ZIO, ZManaged}
import zio.spark.effect.NewType.{Ratio, Weight}
import zio.spark.rdd.RDD
import zio.stream.ZStream

import scala.util.Either

object MapWithEffect {

  @SuppressWarnings(Array("scalafix:DisableSyntax.defaultArgs"))
  def apply[E1, E2 >: E1, A](rdd: RDD[IO[E1, A]])(
      onRejected: E2,
      maxErrorRatio: Ratio = Ratio.p05,
      decayScale: Weight = Weight(1000)
  ): RDD[Either[E2, A]] = rdd.mapZIO(identity, _ => onRejected, maxErrorRatio, decayScale)

  implicit class RDDOps[T](private val rdd: RDD[T]) extends AnyVal {
    @SuppressWarnings(Array("scalafix:DisableSyntax.defaultArgs"))
    def mapZIO[E, B](
        effect: T => IO[E, B],
        onRejection: T => E,
        maxErrorRatio: Ratio = Ratio.p05,
        decayScale: Weight = Weight(1000)
    ): RDD[Either[E, B]] =
      rdd.mapPartitions(
        { it: Iterator[T] =>
          type EE = Option[E]
          val createCircuit: UIO[CircuitTap[EE, EE]] =
            CircuitTap.make[EE, EE](maxErrorRatio, _ => true, None, decayScale)

          def iterator(circuitTap: CircuitTap[EE, EE]): ZManaged[Any, Nothing, Iterator[Either[E, B]]] = {
            val in: ZStream[Any, Nothing, T] = ZStream.fromIterator(it).refineOrDie(PartialFunction.empty)
            val out: ZStream[Any, Nothing, Either[E, B]] = in.mapZIO { x =>
                val exe: ZIO[Any, Option[E], B] = circuitTap(effect(x).asSomeError)
                exe.mapError(_.getOrElse(onRejection(x))).either
              }
            out.toIterator.map(_.map(_.merge))
          }

          val managed: ZManaged[Any, Nothing, Iterator[Either[E, B]]] = createCircuit.toManaged flatMap iterator
          zio.Runtime.global.unsafeRun(managed.use(UIO(_)))
        },
        true
      )
  }
}
