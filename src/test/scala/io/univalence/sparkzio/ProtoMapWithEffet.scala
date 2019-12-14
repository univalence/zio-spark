package io.univalence.sparkzio

import java.util.concurrent.{ BlockingQueue, SynchronousQueue, TimeUnit }

import io.univalence.sparktest.SparkTest
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Dataset, SparkSession }
import org.scalatest.FunSuite
import zio._
import zio.clock.Clock
import zio.duration.Duration
import zio.stream._
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
      r <- if (s.decayingErrorRatio.ratio.value > errBound.value) {
        state.update(_.incRejected) *> rejected.fail
      } else {
        run(effect)
      }
    } yield r

  private def run[R, E >: E2 <: E1, A](effect: ZIO[R, E, A]): ZIO[R, E, A] =
    effect.tapBoth({
      case e if qualified(e) => state.update(_.incFailure)
      case _                 => state.update(_.incSuccess)
    }, _ => state.update(_.incSuccess))

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

  def times(r1: Ratio, w1: Int, r2: Ratio, w2: Int): Ratio =
    create((r1.value * w1 + r2.value * w2) / (w1 + w2))

}

case class DecayingRatio(ratio: Ratio, scale: Int) {
  def decay(value: Ratio, maxScale: Long): DecayingRatio =
    DecayingRatio(Ratio.times(ratio, scale, value, 1), if (scale < maxScale) scale else maxScale.toInt)
}

object Tap {

  case class State(failed: Long, success: Long, rejected: Long, decayingErrorRatio: DecayingRatio) {

    private def nextErrorRatio(ratio: Ratio): DecayingRatio =
      if (total == 0) DecayingRatio(ratio, decayingErrorRatio.scale)
      else decayingErrorRatio.decay(ratio, total)

    def total: Long = failed + success + rejected

    def incFailure: State = copy(failed = failed + 1, decayingErrorRatio = nextErrorRatio(Ratio.full))

    def incSuccess: State = copy(success = success + 1, decayingErrorRatio = nextErrorRatio(Ratio.zero))

    def incRejected: State = copy(rejected = rejected + 1, decayingErrorRatio = nextErrorRatio(Ratio.zero))

    def totalErrorRatio: Ratio = Ratio.apply(failed.toDouble / total).getOrElse(Ratio.zero)

  }

  def zeroState(scale: Int) = State(0, 0, 0, DecayingRatio(Ratio.zero, scale))

  /**
    * Creates a tap that aims for the specified
    * maximum error rate, using the specified
    * function to qualify errors (unqualified
    * errors are not treated as failures for
    * purposes of the tap), and the specified
    * default error used for rejecting tasks
    * submitted to the tap.
    */
  def make[E1, E2](maxError: Ratio, qualified: E1 => Boolean, rejected: => E2, decayScale: Int): UIO[Tap[E1, E2]] =
    for {
      state <- Ref.make[State](zeroState(decayScale))
    } yield {
      new SmartTap[E1, E2](maxError, qualified, rejected, state)
    }
}

object syntax {

  implicit class ToTask[A](t: Try[A]) {
    def toTask: Task[A] = Task.fromTry(t)
  }
}

object ProtoMapWithEffetTest {

  def putStrLn(line: String): UIO[Unit] = zio.console.putStrLn(line).provide(console.Console.Live)

  def tap[E1, E2 >: E1, A](
    rddIO: RDD[IO[E1, A]]
  )(onRejected: E2,
    maxErrorRatio: Ratio      = Ratio(0.05).get,
    keepOrdering: Boolean     = false,
    decayScale: Int           = 1000,
    localConcurrentTasks: Int = 4): RDD[Either[E2, A]] =
    rddIO.mapPartitions(it => {

      val in: stream.Stream[Nothing, IO[E1, A]] = zio.stream.Stream.fromIterator(it.succeed)

      val circuitBreaked: ZIO[Any, Nothing, ZStream[Any, Nothing, Either[E2, A]]] = for {
        tap <- Tap.make[E2, E2](maxErrorRatio, _ => true, onRejected, decayScale)
      } yield {
        if (keepOrdering)
          in.mapMPar(localConcurrentTasks)(x => tap(x).either)
        else
          in.mapMParUnordered(localConcurrentTasks)(x => tap(x).either)

      }

      ToIterator.withNewRuntime.unsafeCreate(circuitBreaked.orDie)
    })

}

class ProtoMapWithEffetTest extends FunSuite with SparkTest {

  import ProtoMapWithEffetTest._

  test("1") {

    val someThing: RDD[Task[Int]] = ss.sparkContext.parallelize(1 to 100).map(x => Task(x))

    val executed: RDD[Either[Throwable, Int]] = tap(someThing)(new Exception("rejected"))

    assert(executed.count() == 100)

  }

  def time[R](block: => R): (Duration, R) = {
    val t0     = System.nanoTime()
    val result = block // call-by-name
    val t1     = System.nanoTime()
    (Duration(t1 - t0, TimeUnit.NANOSECONDS), result)
  }

  test("2") {

    val n                = 500
    val ds: Dataset[Int] = ss.createDataset(1 to n)

    def duration(i: Int) = Duration(if (i % 20 == 0 && i < 200) 800 else 10, TimeUnit.MILLISECONDS)

    def io(i: Int): IO[String, Int] = IO.fail(s"e$i").delay(duration(i)).provide(Clock.Live)

    val value: RDD[IO[String, Int]] = ds.rdd.map(io)

    val unit: RDD[Either[String, Int]] =
      tap(value)(
        onRejected           = "rejected",
        maxErrorRatio        = Ratio(0.10).get,
        keepOrdering         = false,
        localConcurrentTasks = 8
      )

    val (d, _) = time(assert(unit.count() == n))

    val computeTime: Long = (1 to n).map(duration).reduce(_ + _).toMillis

    val speedUp = computeTime.toDouble / d.toMillis

    println(s"speedUp of $speedUp")
  }

  test("asyncZIO") {

    val n                       = 50
    val s: Stream[Nothing, Int] = stream.Stream.fromIterator(UIO((1 to n).toIterator))

    def effect(i: Int): ZIO[Any, String, String] = if (i % 4 == 0) s"f$i".fail else s"s$i".succeed

    val g = s.map(effect)

    val h = for {
      tap <- Tap.make[String, String](Ratio.full, _ => true, "rejected", 1000)
    } yield {
      g.mapMParUnordered(4)(i => tap(i).either)
    }

    val xs: Seq[Either[String, String]] = ToIterator.withNewRuntime.unsafeCreate(h).toSeq

    assert(xs.length == n)

  }

  test("tap") {

    import syntax._

    val e3: IO[String, String] = "third".fail

    val prg: Task[(Either[String, String], Tap.State)] = for {
      percent <- Ratio(0.05).toTask
      tap     <- Tap.make[String, String](percent, _ => true, "rejected", 1000)
      _       <- tap("first".fail).ignore
      _       <- tap.getState.flatMap(s => ZIO.effect(println(s)))
      _       <- tap("second".fail).ignore
      a       <- tap(e3).either
      s       <- tap.getState
    } yield {
      (a, s)
    }

    val runtime = new DefaultRuntime {}
    println(runtime.unsafeRunSync(prg))

  }

}
