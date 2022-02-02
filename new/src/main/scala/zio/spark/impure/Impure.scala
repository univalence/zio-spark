package zio.spark.impure

import zio.Task

/** Give the ability for a type T to execute Impure code. */
trait Impure[+T] {

  /** The underlying instance of type T. */
  protected def impure: T

  /** Apply a function to the type T in an impure way. */
  final def execute[B](f: T => B): Task[B] = Task(f(impure))

  /** Apply a function to the type T in an impure way. */
  final def executeBlocking[B](f: T => B): Task[B] = Task.attemptBlocking(f(impure))
}
