package zio.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import zio._
import zio.console.Console
import zio.stream._
import zio.test._

import scala.util._

object syntax {

  implicit class ValueOps[T](t: T) {
    def fail: IO[T, Nothing] = IO.fail(t)
  }

  implicit class ToTask[A](t: Try[A]) {
    def toTask: Task[A] = Task.fromTry(t)
  }
}

object ProtoMapWithEffetTest extends DefaultRunnableSpec {

  def putStrLn(line: String): ZIO[Console, Nothing, Unit] = zio.console.putStrLn(line)

  def tap[E1, E2 >: E1, A](
    rddIO: RDD[IO[E1, A]]
  )(
    onRejected: E2,
    maxErrorRatio: Ratio = Ratio(0.05).get,
    keepOrdering: Boolean = false,
    decayScale: Int = 1000,
    localConcurrentTasks: Int = 4
  ): RDD[Either[E2, A]] =
    rddIO.mapPartitions(it => {

      val in: Stream[Nothing, IO[E1, A]] = zio.stream.Stream.fromIterator(UIO(it))

      val circuitBreaked: ZIO[Any, Nothing, ZStream[Any, Nothing, Either[E2, A]]] = for {
        tap <- CircuitTap.make[E2, E2](maxErrorRatio, _ => true, onRejected, decayScale)
      } yield {
        if (keepOrdering)
          in.mapMPar(localConcurrentTasks)(x => tap(x).either)
        else
          in.mapMParUnordered(localConcurrentTasks)(x => tap(x).either)

      }

      val iterator: ZIO[Any, Nothing, Iterator[Nothing, Either[E2, A]]] =
        Iterator.unwrapManaged(circuitBreaked.toManaged_ >>= Iterator.fromStream)

      zio.Runtime.default.unsafeRunTask(iterator)

    })

  import ProtoMapWithEffetTest._

  import zio.test._

  val ss: Task[SparkSession] = Task(SparkSession.builder().master("local[*]").appName("toto").getOrCreate())

  override def spec: ZSpec[zio.test.environment.TestEnvironment, Any] =
    suite("proto map with effet")(
      testM("1") {
        ss.map(ss => {

          val someThing: RDD[Task[Int]] = ss.sparkContext.parallelize(1 to 100).map(x => Task(x))

          val executed: RDD[Either[Throwable, Int]] = tap(someThing)(new Exception("rejected"))

          assert(executed.count())(Assertion.equalTo(100L))
        })
      }
    )
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
