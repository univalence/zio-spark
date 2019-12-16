package io.univalence.sparkzio

import zio.stream.ZStream
import zio.{Exit, URIO, ZIO, ZManaged}

import scala.annotation.tailrec

/**
  * Iterator[+E, +A] allows to have an error "channel", on top of scala.collection.Iterator[+A]
  *
  * When an error has been raised (lastError.isDefined), the Iterator is empty (!hasNext)
  *
  * @tparam E Type of the error "channel"
  * @tparam A Type of the value
  */
trait Iterator[+E, +A] extends scala.Iterator[A] {
  def lastError: Option[E]

  //if(lastError.isDefined) assert(!hasNext)
}

object Iterator {

  def wrapIterator[A](iterator: scala.collection.Iterator[A]): Iterator[Nothing, A] = new Iterator[Nothing, A] {
    override def lastError: Option[Nothing] = None
    override def hasNext: Boolean           = iterator.hasNext
    override def next(): A                  = iterator.next()
  }

  private val undefinedBehavior = new NoSuchElementException("next on empty iterator")

  sealed private trait State[+E, +V]
  private object State {
    case object Running extends State[Nothing, Nothing]
    sealed trait ValueOrClosed[+E, +V] extends State[E, V]
    case class Closed[+E](lastError: Option[E]) extends ValueOrClosed[E, Nothing]
    case class Value[+V](value: V) extends ValueOrClosed[Nothing, V]
  }

  private case class EmptyIterator[+E](lastError: Option[E]) extends Iterator[E, Nothing] {
    override def hasNext: Boolean = false
    override def next(): Nothing  = throw undefinedBehavior
  }

  def absolve[E, A](either: Either[E, Iterator[E, A]]): Iterator[E, A] = either match {
    case Left(e)  => EmptyIterator(Some(e))
    case Right(i) => i
  }

  def onLastElement[R, E, A](
    iterator: Iterator[E, A]
  )(onClose: Exit[E, Any] => URIO[R, Any]): URIO[R, Iterator[E, A]] =
    ZIO
      .runtime[R]
      .map(
        runtime =>
          new Iterator[E, A] {
            override def lastError: Option[E] = iterator.lastError

            private var isOpen = true

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
              if (!hasNext) close()
              hasNext
            }

            override def next(): A =
              try {
                iterator.next()
              } catch {
                case e: Throwable =>
                  close()
                  throw e
              }
        }
      )

  def unwrapManaged[R, E1, E2, A](zManaged: ZManaged[R, E1, Iterator[E2, A]]): ZIO[R, E1, Iterator[E2, A]] =
    for {
      reservation <- zManaged.reserve
      acquired    <- reservation.acquire
      iterator    <- onLastElement(acquired)(reservation.release)
    } yield {
      iterator
    }

  def fromStream[R, E, A](zStream: ZStream[R, E, A]): URIO[R, Iterator[E, A]] = {
    val zManaged: ZManaged[R, E, Iterator[E, A]] = for {
      pull    <- zStream.process
      runtime <- ZIO.runtime[R].toManaged_
    } yield {
      new Iterator[E, A] {
        import Iterator.State._

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

    unwrapManaged(zManaged).either.map(absolve)
  }
}
