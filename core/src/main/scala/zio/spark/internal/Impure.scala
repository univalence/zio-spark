package zio.spark.internal

import zio.{RIO, Task, UIO, ZIO}
import zio.spark.internal.Impure.ImpureBox

import scala.util.Try

/** Give the ability for a type T to attempt Impure code. */
abstract class Impure[+A](underlying: ImpureBox[A]) {
  final def attempt[B](f: A => B): Task[B] = underlying.attempt(f)

  final def attemptBlocking[B](f: A => B): Task[B] = underlying.attemptBlocking(f)

  final def attemptWithZIO[R, B](f: A => RIO[R, B]): RIO[R, B] = underlying.attemptWithZIO(f)
}

object Impure {

  implicit final class ImpureBox[+A](private val value: A) extends AnyVal {
    def attempt[B](f: A => B): Task[B] = Task(f(value))

    def attemptBlocking[B](f: A => B): Task[B] = Task.attemptBlocking(f(value))

    def attemptWithZIO[R, B](f: A => RIO[R, B]): RIO[R, B] = f(value)

    private[spark] def attemptNow[B](f: A => B): Try[B] = Try(f(value))

    private[spark] def succeed[B, C](f: A => B): UIO[B] = ZIO.succeed(f(value))

    private[spark] def succeedWithZIO[R, E, B](f: A => ZIO[R, E, B]): ZIO[R, E, B] = f(value)

    private[spark] def succeedNow[B](f: A => B): B = f(value)
  }

}
