package zio.spark.effect

import zio.{IO, UIO, ZManaged}
import zio.spark.rdd.RDD
import zio.stream.ZStream

import scala.util.Either

object MapWithEffect {

  @SuppressWarnings(Array("scalafix:DisableSyntax.defaultArgs"))
  def apply[E1, E2 >: E1, A](rdd: RDD[IO[E1, A]])(
      onRejected: E2,
      maxErrorRatio: Ratio = Ratio.p05,
      decayScale: Int = 1000
  ): RDD[Either[E2, A]] =
    rdd.mapPartitions(
      { it =>
        val createCircuit: UIO[CircuitTap[E2, E2]] =
          CircuitTap.make[E2, E2](maxErrorRatio, _ => true, onRejected, decayScale)

        def iterator(circuitTap: CircuitTap[E2, E2]): ZManaged[Any, Nothing, Iterator[Either[E2, A]]] = {
          val in: ZStream[Any, Nothing, IO[E1, A]]      = ZStream.fromIterator(it).refineOrDie(PartialFunction.empty)
          val out: ZStream[Any, Nothing, Either[E2, A]] = in.mapZIO(x => circuitTap(x).either)
          out.toIterator.map(_.map(_.merge))
        }

        val managed: ZManaged[Any, Nothing, Iterator[Either[E2, A]]] = createCircuit.toManaged flatMap iterator

        zio.Runtime.global.unsafeRun(managed.use(x => UIO(x)))
      },
      true
    )

  implicit class RDDOps[T](private val rdd: RDD[T]) extends AnyVal {
    @SuppressWarnings(Array("scalafix:DisableSyntax.defaultArgs"))
    def mapZIO[E, B](
        effect: T => IO[E, B],
        onRejection: T => E,
        maxErrorRatio: Ratio = Ratio.p05,
        decayScale: Int = 1000
    ): RDD[Either[E, B]] = ???
  }
}
