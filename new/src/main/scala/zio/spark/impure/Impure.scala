package zio.spark.impure

import zio.{RIO, Task, UIO, ZIO}

import scala.util.Try

/** Give the ability for a type T to execute Impure code. */
//TODO: rename execute into attempt
//TODO: rename attemptSuccess into succeed
//TODO: change trait to abstract class
//TODO: change protected def impure into a private


trait Impure[+T] {

  /** The underlying instance of type T. */
  protected def impure: T

  /** Apply a function to the type T in an impure way. */
  final def execute[B](f: T => B): Task[B] = Task(f(impure))

  final def executeBlocking[B](f: T => B): Task[B] = Task.attemptBlocking(f(impure))

  final def executeWithZIO[R, B](f: T => RIO[R, B]): RIO[R, B] = f(impure)

  final protected def executeSuccess[B, C](f: T => B): UIO[B] = UIO(f(impure))

  final protected def executeSuccessWithZIO[R, E, B](f: T => ZIO[R, E, B]): ZIO[R, E, B] = f(impure)

  final protected def executeSuccessNow[B](f: T => B): B = f(impure)

  final protected def executeNow[B](f: T => B): Try[B] = Try(f(impure))
}
