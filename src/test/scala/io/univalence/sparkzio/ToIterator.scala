package io.univalence.sparkzio

import zio.{ stream, Cause, DefaultRuntime, Exit, Runtime, UIO }

class ToIterator private (runtime: Runtime[_]) {
  def unsafeCreate[E, V](q: UIO[stream.Stream[E, V]]): Iterator[V] =
    new Iterator[V] {

      import ToIterator._

      var state: State[V] = Running

      private val stream = q.map(_.map(Value.apply))

      private val reservation = runtime.unsafeRun(stream.toManaged_.flatMap(_.process).reserve)

      private val pull = runtime.unsafeRun(reservation.acquire)

      private def pool(): ValueOrClosed[V] =
        state match {
          //stay closed
          case Closed => Closed
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

            element.fold(_ => Closed, x => x)
        }

      override def hasNext: Boolean =
        state match {
          case Closed   => false
          case Value(_) => true
          case Running =>
            state = pool()
            state != Closed
        }

      private val undefinedBehavior = new NoSuchElementException("next on empty iterator")

      override def next(): V =
        state match {
          case Value(value) =>
            state = Running
            value
          case Closed => throw undefinedBehavior
          case Running =>
            pool() match {
              case Closed =>
                state = Closed
                throw undefinedBehavior
              case Value(v) =>
                //state = Running
                v
            }
        }
    }

}

object ToIterator {
  sealed private trait State[+V]
  private case object Running extends State[Nothing]
  sealed private trait ValueOrClosed[+V] extends State[V]
  private case object Closed extends ValueOrClosed[Nothing]
  private case class Value[+V](value: V) extends ValueOrClosed[V]

  def withNewRuntime: ToIterator = new ToIterator(new DefaultRuntime {})

  def withRuntime(runtime: Runtime[_]): ToIterator = new ToIterator(runtime)
}
