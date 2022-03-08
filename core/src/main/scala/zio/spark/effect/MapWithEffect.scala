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
        { it =>
          val createCircuit: UIO[DylanCircuitTap[T, E]] =
            CircuitTap.dylan[T, E](maxErrorRatio, _ => true, onRejection, decayScale)

          def iterator(circuitTap: DylanCircuitTap[T, E]): ZManaged[Any, Nothing, Iterator[Either[E, B]]] = {
            val in: ZStream[Any, Nothing, T] = ZStream.fromIterator(it).refineOrDie(PartialFunction.empty)
            val out: ZStream[Any, E, B] = in.mapZIO { x => circuitTap(x, effect)}
            out.toIterator
          }

          val managed: ZManaged[Any, Nothing, Iterator[Either[E, B]]] = createCircuit.toManaged flatMap iterator
          zio.Runtime.global.unsafeRun(managed.use(UIO(_)))
        },
        true
      )
  }
}
