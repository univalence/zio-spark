package io.univalence.sparkzio

import zio.{ Fiber, Promise, Queue, Ref, Semaphore, UIO, ZIO }
import zio.stream.ZStream
import zio.stream.ZStream.Pull

object ZDynamicConcurrency {

  private def releaseCapacity(semaphore: Semaphore, toRelease: UIO[Int]): UIO[Fiber[Nothing, Nothing]] =
    (for {
      concurrency <- Ref.make(0)
    } yield {
      def update(next: Int): UIO[Unit] =
        (for {
          current <- concurrency.get
          _       <- if (next >= current) semaphore.releaseN(next - current) else semaphore.acquireN(current - next)
          _       <- concurrency.set(next)
        } yield {}).uninterruptible

      val nextConcurrencyBounded: UIO[Int] = toRelease.map({
        case x if x < 0 => 0
        case x          => x
      })

      (nextConcurrencyBounded >>= update).forever.ensuring(update(0)).fork
    }).flatten

  implicit class Ops[-R, +E, +A](stream: ZStream[R, E, A]) {
    def mapMDynamicPar[R1 <: R, E1 >: E, B](concurrencyLevel: UIO[Int],
                                            maxConcurrency: Int = 16)(f: A => ZIO[R1, E1, B]): ZStream[R1, E1, B] =
      ZStream[R1, E1, B] {
        for {
          out                    <- Queue.bounded[Pull[R1, E1, B]](maxConcurrency).toManaged(_.shutdown)
          permits                <- Semaphore.make(permits = 0).toManaged_
          updateConcurrencyFiber <- releaseCapacity(permits, concurrencyLevel).toManaged_
          interruptWorkers       <- Promise.make[Nothing, Unit].toManaged_
          _ <- stream.foreachManaged { a =>
            for {
              latch <- Promise.make[Nothing, Unit]
              p     <- Promise.make[E1, B]
              _     <- out.offer(Pull.fromPromise(p))
              _     <- (permits.withPermit(latch.succeed(()) *> f(a).to(p)) race interruptWorkers.await).fork
              _     <- latch.await
            } yield ()
          }.foldCauseM(
              c => (interruptWorkers.succeed(()) *> out.offer(Pull.halt(c))).unit.toManaged_,
              _ => out.offer(Pull.end).unit.toManaged_
            )
            .ensuringFirst(interruptWorkers.succeed(()) *> updateConcurrencyFiber.interrupt)
            .fork
        } yield out.take.flatten
      }
  }
}
