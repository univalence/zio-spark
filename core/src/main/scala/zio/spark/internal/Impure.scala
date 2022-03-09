package zio.spark.internal

import zio.spark.internal.Impure.ImpureBox
import zio.{Task, UIO, ZIO}

/** Give the ability for a type T to attempt Impure code. */
abstract class Impure[+A](underlying: ImpureBox[A]) {
  final def attempt[B](f: A => B): Task[B] = underlying.attempt(f)

  final def attemptBlocking[B](f: A => B): Task[B] = underlying.attemptBlocking(f)
}

object Impure {

  final case class ImpureBox[+A](private val value: A) {
    def attempt[B](f: A => B): Task[B] = Task(f(value))

    def attemptBlocking[B](f: A => B): Task[B] = Task.attemptBlocking(f(value))

    private[spark] def succeed[B, C](f: A => B): UIO[B] = ZIO.succeed(f(value))

    private[spark] def succeedNow[B](f: A => B): B = f(value)
  }

}
