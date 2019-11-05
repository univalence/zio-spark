package io.univalence.sparkzio

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import zio._
import zio.stream.ZStream.Pull
import zio.syntax._

import scala.collection.immutable

/**
  * A `Tap` adjusts the flow of tasks through
  * an external service in response to observed
  * failures in the service, always trying to
  * maximize flow while attempting to meet the
  * user-defined upper bound on failures.
  */
trait Tap[-E1, +E2] {

  /**
    * Sends the task through the tap. The
    * returned task may fail immediately with a
    * default error depending on the service
    * being guarded by the tap.
    */
  def apply[R, E >: E2 <: E1, A](effect: ZIO[R, E, A]): ZIO[R, E, A]
}

class SmartTap[-E1, +E2](errBound: Percentage, qualified: E1 => Boolean, rejected: => E2, state: Ref[Tap.State])
    extends Tap[E1, E2] {

  override def apply[R, E >: E2 <: E1, A](effect: ZIO[R, E, A]): ZIO[R, E, A] =
    for {
      _ <- state.update(f => f.copy(total = f.total + 1))
      s <- state.get
      r <- if (s.failed / s.total * 100 > errBound.value) {
        ZIO.fail(rejected)
      } else {
        run(effect)
      }
    } yield r

  private def run[R, E >: E2 <: E1, A](effect: ZIO[R, E, A]): ZIO[R, E, A] =
    for {
      f <- effect.fork
      r <- f.join.catchSome {
        case e if qualified(e) =>
          state.update(s => s.copy(failed = s.failed + 1)) *> IO.fail(e)
      }
    } yield r

}

final class Percentage private (val value: Double) extends Ordered[Percentage] {
  override def compare(that: Percentage): Int = this.value.compareTo(that.value)
  override def toString: String               = value.toString
}
object Percentage {
  def apply(n: Double): UIO[Percentage] =
    if (n < 0 || n > 100) IO.die(new IllegalArgumentException("A percentage must be between 0 and 100 included"))
    else new Percentage(n).succeed
}

object Tap {

  case class State(total: Double = 0, failed: Double = 0)

  /**
    * Creates a tap that aims for the specified
    * maximum error rate, using the specified
    * function to qualify errors (unqualified
    * errors are not treated as failures for
    * purposes of the tap), and the specified
    * default error used for rejecting tasks
    * submitted to the tap.
    */
  def make[E1, E2](errBound: Percentage, qualified: E1 => Boolean, rejected: => E2): UIO[Tap[E1, E2]] =
    for {
      state <- Ref.make[State](State())
    } yield new SmartTap[E1, E2](errBound, qualified, rejected, state)
}

class ToIterator private (runtime: Runtime[Any]) {
  def unsafeCreate[E, A](q: UIO[stream.Stream[E, A]]): Iterator[Either[E, A]] =
    new Iterator[Either[E, A]] {

      import ToIterator._

      type V = Either[E, A]

      var state: State[V] = Running

      def pullToEither(either: Either[Option[E], A]): ValueOrClosed[V] =
        either match {
          case Left(Some(x)) => Value(Left(x))
          case Left(None)    => Closed
          case _             => Value(either.asInstanceOf[V])
        }

      val reservation: Reservation[Any, E, Pull[Any, E, A]] = runtime.unsafeRun(q.toManaged_.flatMap(_.process).reserve)

      val pull: IO[Option[E], A] = runtime.unsafeRun(reservation.acquire)

      private def pool() =
        if (state != Closed) {
          val element: Either[Option[E], A] = runtime.unsafeRun(
            pull
              .catchSome({ case None => reservation.release(Exit.Success({})) *> None.fail })
              .either
          )

          pullToEither(element)
        } else {
          Closed
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
                //stage = Running
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

  def withRuntime(runtime: Runtime[Any]): ToIterator = new ToIterator(runtime)
}

class ProtoMapWithEffetTest extends FunSuite {

  def tap[E1, E2 >: E1, A](rddIO: RDD[IO[E1, A]])(onRejected: E2): RDD[Either[E2, A]] =
    rddIO.mapPartitions(it => {

      val compose = for {
        p         <- Percentage(99.9)
        tap       <- Tap.make[E2, E2](p, _ => true, onRejected)
        allOfThem <- IO.collectAll(it.map(x => tap.apply[Any, E2, A](x).either).toIterable)

      } yield allOfThem

      val runtime = new DefaultRuntime {}

      val list: immutable.Seq[Either[E2, A]] = runtime.unsafeRun(compose)

      list.toIterator
    })

  def tap2[E1, E2 >: E1, A](rddIO: RDD[IO[E1, A]])(onRejected: E2): RDD[Either[E2, A]] =
    rddIO.mapPartitions(it => {

      val in: stream.Stream[Nothing, IO[E1, A]] = zio.stream.Stream.fromIterator(it.succeed)

      val circuitBreaked: UIO[stream.Stream[E2, A]] = for {
        percent <- Percentage(99.9)
        tap     <- Tap.make[E2, E2](percent, _ => true, onRejected)
        stream = in.mapMPar(3)(x => tap.apply(x))
      } yield {
        stream
      }

      ToIterator.withNewRuntime.unsafeCreate(circuitBreaked)
    })

  val ss: SparkSession = SparkSession.builder().master("local").appName("test").getOrCreate()

  test("1") {

    val someThing: RDD[Task[Int]] = ss.sparkContext.parallelize(1 to 100).map(x => Task(x))

    val executed: RDD[Either[Throwable, Int]] = tap2(someThing)(new Exception("rejected"))

    assert(executed.count() == 100)

  }

}
