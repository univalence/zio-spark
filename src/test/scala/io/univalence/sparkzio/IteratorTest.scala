package io.univalence.sparkzio

import zio.{ DefaultRuntime, IO, Ref, Task, UIO, ZIO, ZManaged }
import zio.clock.Clock
import zio.stream.{ Stream, ZSink, ZStream }
import zio.test.DefaultRunnableSpec
import zio.test._
import zio.test.Assertion._

object StreamTest {

  def assertForAll[R, E, A](zstream: ZStream[R, E, A])(f: A => TestResult): ZIO[R, E, TestResult] =
    zstream.fold(assert(Unit, Assertion.anything))((as, a) => as && f(a))

  def isSorted[A: Ordering]: Assertion[Iterable[A]] =
    Assertion.assertion("sorted")()(x => {
      val y = x.toList
      y.sorted == y
    })
}

object IteratorTest
    extends DefaultRunnableSpec(
      suite("iterator")(
        testM("to iterator should be lazy")({
          case class Element(n: Int, time: Long)

          (for {
            clock      <- ZIO.environment[Clock]
            n          <- Ref.make(0)
            incCounter <- n.update(_ + 1).forever.fork

          } yield {
            def element: UIO[Element] = n.get.zipWith(clock.clock.nanoTime)(Element)

            val in = Stream.repeatEffect(element)

            val iterator = Iterator.unwrapManaged(Iterator.fromStream(in))

            val out: ZStream[Any, Nothing, Element] =
              ZStream.fromIterator(iterator).mapConcatM(e => element.map(List(e, _)))

            implicit val ordering: Ordering[Element] = Ordering.by(x => x.n -> x.time)

            out.take(2000).runCollect.map(e => assert(e, StreamTest.isSorted))
          }).flatten
        }),
        testM("<=>")({
          val in: List[Int] = (1 to 100).toList

          (for {
            _ <- UIO.unit.toManaged_
            stream1 = ZStream.fromIterator(UIO(in.toIterator))
            iterator <- Iterator.fromStream(stream1)
            stream2 = ZStream.fromIterator(UIO(iterator))
            out <- stream2.runCollect.toManaged_
          } yield {
            assert(in, equalTo(out))
          }).use(x => ZIO.effect(x))

        }),
        testM("on exit")(
          (for {
            isOpen <- Ref.make(false).toManaged_
            stream = ZStream.managed(ZManaged.make(isOpen.update(_ => true))(_ => isOpen.set(false)))
            iterator <- Iterator.fromStream(stream)
          } yield {
            assert(iterator.toList, equalTo(List(true)))
          }).use(x => IO.effect(x))
        )
      )
    )
