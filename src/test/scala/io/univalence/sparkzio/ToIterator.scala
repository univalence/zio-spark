package io.univalence.sparkzio

import zio.{ stream, Cause, DefaultRuntime, Exit, Runtime, UIO, ZIO, ZManaged }

class ToIterator[R] private (runtime: Runtime[R]) {
  import ToIterator._
  import State._
  def unsafeCreate[E, V](q: UIO[stream.Stream[E, V]]): Iterator[E, V] =
    new Iterator[E, V] {
      var state: State[E, V] = Running

      private val stream = q.map(_.map(Value.apply))

      private val reservation = runtime.unsafeRun(stream.toManaged_.flatMap(_.process).reserve)

      private val pull = runtime.unsafeRun(reservation.acquire)

      private def pool(): ValueOrClosed[E, V] =
        state match {
          //stay closed
          case closed: Closed[E] => closed
          case _ =>
            val element: Either[Option[Cause[E]], Value[V]] =
              runtime.unsafeRun(pull.mapErrorCause(x => Cause.fail(Cause.sequenceCauseOption(x))).either)

            element match {
              // if stream is closed, release the stream
              case Left(None) =>
                runtime.unsafeRun(reservation.release(Exit.Success({})))
              case Left(Some(e)) =>
                runtime.unsafeRun(reservation.release(Exit.Failure(e)))
              case _ =>
            }

            element.fold(e => Closed(e.flatMap(_.failureOption)), x => x)
        }

      override def hasNext: Boolean =
        state match {
          case Closed(_) => false
          case Value(_)  => true
          case Running =>
            state = pool()
            state.notClosed
        }

      override def next(): V =
        state match {
          case Value(value) =>
            state = Running
            value
          case Closed(_) => throw undefinedBehavior
          case Running =>
            pool() match {
              case c: Closed[E] =>
                state = c
                throw undefinedBehavior
              case Value(v) =>
                //state = Running
                v
            }
        }

      override def lastError: Option[E] = state match {
        case Closed(e) => e
        case _         => None
      }
    }

}

object ToIterator {
  private val undefinedBehavior = new NoSuchElementException("next on empty iterator")

  sealed private trait State[+E, +V] {
    def notClosed: Boolean = true
  }
  private object State {
    case object Running extends State[Nothing, Nothing]
    sealed trait ValueOrClosed[+E, +V] extends State[E, V]
    case class Closed[+E](lastError: Option[E]) extends ValueOrClosed[E, Nothing] {
      override def notClosed: Boolean = false
    }
    case class Value[+V](value: V) extends ValueOrClosed[Nothing, V]

  }
  @deprecated
  def withNewRuntime: ToIterator[Any] = new ToIterator(new DefaultRuntime {})

  @deprecated
  def withRuntime[R](runtime: Runtime[R]): ToIterator[R] = new ToIterator(runtime)

  trait Iterator[+E, +A] extends scala.Iterator[A] {
    def lastError: Option[E]
  }

  case class EmptyIterator[+E](lastError: Option[E]) extends Iterator[E, Nothing] {
    override def hasNext: Boolean = false
    override def next(): Nothing  = throw undefinedBehavior
  }

  def mergeError[E, A](either: Either[E, Iterator[E, A]]): Iterator[E, A] = either match {
    case Left(e)  => EmptyIterator(Some(e))
    case Right(i) => i
  }

  //def unManaged[R, E1, E2, A](ZManaged: ZManaged[R, E1, Iterator[E2, A]]): ZIO[R, E1, Iterator[E2, A]] = ???

  //def apply[R, E, A](zStream: ZStream[R, E, A]): ZIO[R, Nothing, Iterator[E,A]] = ???
}
