package zio.spark

import zio._
import zio.stream._
import zio.test._

import scala.util._

object syntax {

  implicit class ZIOOps[R, E, A](val zio: ZIO[R, E, A]) extends AnyVal {

    @inline
    def >>-[B](f: A => B): ZIO[R, E, B] = zio.map(f)
  }

  implicit class AnyOps[A](val a: A) extends AnyVal {
    @inline
    def >-[B](f: A => B): B = f(a)
  }

  implicit class ValueOps[T](val t: T) extends AnyVal {
    @inline
    def fail: IO[T, Nothing] = IO.fail(t)
  }

  implicit class ToTask[A](val t: Try[A]) {
    @inline
    def toTask: Task[A] = Task.fromTry(t)
  }
}

object ProtoMapWithEffetTest extends DefaultRunnableSpec with SparkTest {

  protected def unmanaged[R, E, A](zManaged: ZManaged[R, E, A]): ZIO[R, E, A] =
    for {
      r    <- ZIO.environment[R]
      rMap <- ZManaged.ReleaseMap.make
      t    <- zManaged.zio.provide(r -> rMap)
    } yield t._2

  override def spec: ZSpec[zio.test.environment.TestEnvironment, Any] =
    suite("proto map with effet")(
      testM("1") {
        ss.flatMap(ss => {

          val someThing: ZRDD[Task[Int]]             = ss.sparkContext.parallelize(1 to 100).map(x => Task(x))
          val executed: ZRDD[Either[Throwable, Int]] = tap(someThing)(new Exception("rejected"))

          assertM(executed.count)(Assertion.equalTo(100L))

        })
      } @@ max20secondes
    )

  def tap[E1, E2 >: E1, A](
    rddIO: ZRDD[IO[E1, A]]
  )(
    onRejected: E2,
    maxErrorRatio: Ratio = Ratio(0.05).get,
    decayScale: Int = 1000
  ): ZRDD[Either[E2, A]] =
    rddIO.mapPartitions(it => {

      val createCircuit: UIO[CircuitTap[E2, E2]] =
        CircuitTap.make[E2, E2](maxErrorRatio, _ => true, onRejected, decayScale)

      def iterator(circuitTap: CircuitTap[E2, E2]): UManaged[Iterator[Either[E2, A]]] = {
        val in: Stream[Nothing, IO[E1, A]] = ZStream.fromIterator(it).refineOrDie(PartialFunction.empty)
        val out: Stream[E2, A]             = in.mapM(circuitTap.apply)
        out.toIterator
      }

      val managed: ZManaged[Any, Nothing, Iterator[Either[E2, A]]] = createCircuit.toManaged_ >>= iterator

      zio.Runtime.global.unsafeRun(unmanaged(managed))
    })
  /*

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
        onRejected = "rejected",
        maxErrorRatio = Ratio(0.10).get,
        keepOrdering = false,
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

    val h: UIO[Stream[Nothing, Either[String, String]]] = for {
      tap <- CircuitTap.make[String, String](Ratio.full, _ => true, "rejected", 1000)
    } yield {
      g.mapMParUnordered(4)(i => tap(i).either)
    }

    val prg: UIO[Iterator[Nothing, Either[String, String]]] =
      Iterator.unwrapManaged(h.toManaged_ >>= Iterator.fromStream)

    val xs: Seq[Either[String, String]] = new DefaultRuntime {}.unsafeRun(prg).toSeq

    assert(xs.length == n)

  }*/

}
