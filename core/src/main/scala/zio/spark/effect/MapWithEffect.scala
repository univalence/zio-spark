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
          val in: ZStream[Any, Nothing, IO[E1, A]] = ZStream.fromIterator(it).refineOrDie(PartialFunction.empty)
          val out: ZStream[Any, E2, A]             = in.mapZIO(circuitTap.apply)
          out.toIterator
        }

        val managed: ZManaged[Any, Nothing, Iterator[Either[E2, A]]] = createCircuit.toManaged flatMap iterator

        zio.Runtime.global.unsafeRun(managed.use(x => UIO(x)))
      },
      true
    )

}
