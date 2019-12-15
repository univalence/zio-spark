package io.univalence.sparkzio

import org.scalatest.FunSuite
import zio.{ DefaultRuntime, Ref, UIO, ZIO }
import zio.clock.Clock
import zio.stream.{ Stream, ZStream }

import scala.collection.immutable

class ToIteratorTest extends FunSuite {

  test("To Iterator should be lazy") {
    case class Element(n: Int, time: Long)

    val io: ZIO[Clock, Nothing, Stream[Nothing, Element]] = for {
      clock <- ZIO.environment[Clock]
      v     <- Ref.make(0)
      _     <- v.update(_ + 1).forever.fork
    } yield {
      Stream.repeatEffect(v.get.zipWith(clock.clock.nanoTime)(Element))
    }

    val iterator: Iterator[Element] = ToIterator.withNewRuntime.unsafeCreate(io.provide(Clock.Live))

    def testIterator(prevElement: Element): Element = {
      val currentTime = System.nanoTime()
      val element     = iterator.next()
      assert(prevElement.n < element.n)
      assert(currentTime < element.time)
      element
    }

    Iterator.iterate(Element(n = 0, time = 0))(testIterator).take(200).foreach(_ => { Thread.sleep(1) })
  }

  test("<=>") {

    val runtime = new DefaultRuntime {}

    val in: Range.Inclusive = 1 to 100

    val toStream_1: Stream[Nothing, Int] = ZStream(in: _*)

    val toIterator: Iterator[Int] = ToIterator.withRuntime(runtime).unsafeCreate(UIO(toStream_1))

    val toStream_2: Stream[Nothing, Int] = ZStream.fromIterator(UIO(toIterator))

    val out: Seq[Int] = runtime.unsafeRun(toStream_2.runCollect)

    assert(in == out)
  }

}
