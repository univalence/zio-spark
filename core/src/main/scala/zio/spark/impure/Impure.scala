package zio.spark.impure

import zio.{RIO, Task, UIO, ZIO}

import scala.util.Try

/** Give the ability for a type T to attempt Impure code. */
abstract class Impure[+A](private val value: A) {
  final def attempt[B](f: A => B): Task[B] = Task(f(value))

  final def attemptBlocking[B](f: A => B): Task[B] = Task.attemptBlocking(f(value))

  final def attemptWithZIO[R, B](f: A => RIO[R, B]): RIO[R, B] = f(value)

  final protected def attemptNow[B](f: A => B): Try[B] = Try(f(value))

  final protected def succeed[B, C](f: A => B): UIO[B] = ZIO.succeed(f(value))

  final protected def succeedWithZIO[R, E, B](f: A => ZIO[R, E, B]): ZIO[R, E, B] = f(value)

  final protected def succeedNow[B](f: A => B): B = f(value)

}
