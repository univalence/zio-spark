package io.univalence.sparkzio

import zio.stream.ZStream
import zio.{ Cause, Exit, ZIO, ZManaged }

import scala.annotation.tailrec

object ToIterator {
  private val undefinedBehavior = new NoSuchElementException("next on empty iterator")

  sealed private trait State[+E, +V]
  private object State {
    case object Running extends State[Nothing, Nothing]
    sealed trait ValueOrClosed[+E, +V] extends State[E, V]
    case class Closed[+E](lastError: Option[E]) extends ValueOrClosed[E, Nothing]
    case class Value[+V](value: V) extends ValueOrClosed[Nothing, V]
  }

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

  def execOnLast[R, E, A](
    iterator: Iterator[E, A]
  )(onClose: Exit[E, Any] => ZIO[R, Nothing, Any]): ZIO[R, Nothing, Iterator[E, A]] =
    for {
      runtime <- ZIO.runtime[R]
    } yield {
      new Iterator[E, A] {
        private var isOpen                = true
        override def lastError: Option[E] = iterator.lastError

        private def close(): Unit =
          if (isOpen) {
            isOpen = false
            runtime.unsafeRun(onClose(lastError match {
              case None    => Exit.unit
              case Some(e) => Exit.fail(e)
            }))
          }

        override def hasNext: Boolean = {
          val hasNext = iterator.hasNext
          if (!hasNext && isOpen) {
            close()
          }
          hasNext
        }

        override def next(): A =
          try {
            iterator.next()
          } catch {
            case e: Throwable if isOpen =>
              close()
              throw e
          }
      }
    }

  def unManaged[R, E1, E2, A](zManaged: ZManaged[R, E1, Iterator[E2, A]]): ZIO[R, E1, Iterator[E2, A]] =
    for {
      reservation <- zManaged.reserve
      acquired    <- reservation.acquire
      iterator    <- execOnLast(acquired)(reservation.release)
    } yield {
      iterator
    }

  def apply[R, E, A](zStream: ZStream[R, E, A]): ZIO[R, Nothing, Iterator[E, A]] = {
    val zManaged: ZManaged[R, E, Iterator[E, A]] = for {
      pull    <- zStream.process
      runtime <- ZIO.runtime[R].toManaged_
    } yield {
      new Iterator[E, A] {
        import State._

        var state: State[E, A] = Running

        private def pool(): ValueOrClosed[E, A] =
          runtime
            .unsafeRun(pull.either)
            .fold(
              e => State.Closed(e),
              a => State.Value(a)
            )

        override def lastError: Option[E] = state match {
          case Closed(e) => e
          case _         => None
        }

        @tailrec
        override def hasNext: Boolean =
          state match {
            case Closed(_) => false
            case Value(_)  => true
            case Running =>
              state = pool()
              hasNext
          }

        @tailrec
        override def next(): A =
          state match {
            case Closed(_) => throw undefinedBehavior
            case Value(a) =>
              state = Running
              a
            case Running =>
              state = pool()
              next()
          }
      }
    }

    unManaged(zManaged).either.map(mergeError)
  }
}
