package io.univalence.sparkzio

import io.univalence.sparktest.SparkTest
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Dataset, SparkSession }
import org.scalatest.FunSuite
import zio._
import zio.stream._
import zio.stream.ZStream.Pull
import zio.syntax._

import scala.util.{ Failure, Success, Try }

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

  def getState: UIO[Tap.State]
}

class SmartTap[-E1, +E2](errBound: Ratio, qualified: E1 => Boolean, rejected: => E2, state: Ref[Tap.State])
    extends Tap[E1, E2] {

  override def apply[R, E >: E2 <: E1, A](effect: ZIO[R, E, A]): ZIO[R, E, A] =
    for {
      s <- state.get
      r <- if (s.decayingErrorRatio.value > errBound.value) {
        state.update(_.incRejected) *> rejected.fail
      } else {
        run(effect)
      }
    } yield r

  private def run[R, E >: E2 <: E1, A](effect: ZIO[R, E, A]): ZIO[R, E, A] =
    effect
      .foldM({
        case e if qualified(e) => state.update(_.incFailure) *> e.fail
        case e                 => state.update(_.incSuccess) *> e.fail
      }, v => state.update(_.incSuccess) *> v.succeed) -

  override def getState: UIO[Tap.State] = state.get
}

final class Ratio private (val value: Double) extends AnyVal with Ordered[Ratio] {
  override def compare(that: Ratio): Int = this.value.compareTo(that.value)
  override def toString: String          = value.toString
}
object Ratio {

  private def create(value: Double): Ratio = {
    assert(value >= 0.0 && value <= 1)
    new Ratio(value)
  }

  def apply(n: Double): Try[Ratio] =
    if (n < 0.0 || n > 1)
      Failure(new IllegalArgumentException("A ratio must be between 0.0 and 1.0"))
    else
      Success(new Ratio(n))

  val zero: Ratio = Ratio.create(0.0)
  val full: Ratio = Ratio.create(1.0)

  def decay(base: Ratio, value: Ratio, scale: Int): Ratio = {
    assert(scale >= 0)
    new Ratio((base.value * scale + value.value) / (scale + 1))
  }

}

object Tap {

  private val scale = 1000

  case class State(failed: Long, success: Long, rejected: Long, decayingErrorRatio: Ratio) {

    private def nextErrorRatio(ratio: Ratio): Ratio =
      if (total == 0) ratio
      else Ratio.decay(decayingErrorRatio, ratio, if (total < scale) total.toInt else scale)

    def total: Long = failed + success + rejected

    def incFailure: State = copy(failed = failed + 1, decayingErrorRatio = nextErrorRatio(Ratio.full))

    def incSuccess: State = copy(success = success + 1, decayingErrorRatio = nextErrorRatio(Ratio.zero))

    def incRejected: State = copy(rejected = rejected + 1, decayingErrorRatio = nextErrorRatio(Ratio.zero))

    def totalErrorRatio: Ratio = Ratio.apply(failed.toDouble / total).getOrElse(Ratio.zero)

  }

  val zeroState = State(0, 0, 0, Ratio.zero)

  /**
    * Creates a tap that aims for the specified
    * maximum error rate, using the specified
    * function to qualify errors (unqualified
    * errors are not treated as failures for
    * purposes of the tap), and the specified
    * default error used for rejecting tasks
    * submitted to the tap.
    */
  def make[E1, E2](maxError: Ratio, qualified: E1 => Boolean, rejected: => E2): UIO[Tap[E1, E2]] =
    for {
      state <- Ref.make[State](zeroState)
    } yield new SmartTap[E1, E2](maxError, qualified, rejected, state)
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

      private def pool(): ValueOrClosed[V] =
        if (state != Closed) {
          val element: Either[Option[E], A] = runtime.unsafeRun(pull.either)

          element match {
            // if stream is closed, release the stream
            case Left(None) => runtime.unsafeRun(reservation.release(Exit.Success({})))
            case _          =>
          }

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

object syntax {

  implicit class ToTask[A](t: Try[A]) {
    def toTask: Task[A] = Task.fromTry(t)
  }
}

class ProtoMapWithEffetTest extends FunSuite with SparkTest {

  def tap[E1, E2 >: E1, A](rddIO: RDD[IO[E1, A]])(onRejected: E2): RDD[Either[E2, A]] =
    rddIO.mapPartitions(it => {

      val in: stream.Stream[Nothing, IO[E1, A]] = zio.stream.Stream.fromIterator(it.succeed)

      import syntax._

      val circuitBreaked: Task[stream.Stream[E2, A]] = for {
        percent <- Ratio(0.05).toTask
        tap     <- Tap.make[E2, E2](percent, _ => true, onRejected)
      } yield {
        in.mapM(x => tap(x))
      }

      ToIterator.withNewRuntime.unsafeCreate(circuitBreaked.orDie)
    })

  test("1") {

    val someThing: RDD[Task[Int]] = ss.sparkContext.parallelize(1 to 100).map(x => Task(x))

    val executed: RDD[Either[Throwable, Int]] = tap(someThing)(new Exception("rejected"))

    assert(executed.count() == 100)

  }

  test("2") {

    def putStrLn(line: String): UIO[Unit] = IO.effectTotal(println(line))

    val ds: Dataset[Int] = ss.createDataset(0 to 50).coalesce(1)

    val unit: RDD[Either[String, Int]] =
      tap(ds.rdd.map(i => putStrLn("val " + i).flatMap(_ => IO.fail("e").asInstanceOf[IO[String, Int]])))("rejected")

    unit
      .map(x => {
        println(x)
        x.left.get
      })
      .collect()
  }

  test("tap") {

    import syntax._

    val prg = for {
      percent <- Ratio(0.05).toTask
      tap     <- Tap.make[String, String](percent, _ => true, "rejected")
      _       <- tap("first".fail).ignore
      _       <- tap("second".fail).ignore

      a <- tap("thrid".fail.as("value")).either

      s <- tap.getState
    } yield {
      (a, s)
    }

    println(new DefaultRuntime {}.unsafeRunSync(prg.either))

  }

}
