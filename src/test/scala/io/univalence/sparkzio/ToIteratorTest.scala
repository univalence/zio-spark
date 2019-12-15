package io.univalence.sparkzio

import org.scalatest.FunSuite
import zio.{ DefaultRuntime, Ref, Task, UIO, ZIO }
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

    val iterator: ZIO[Clock, Nothing, ToIterator.Iterator[Nothing, Element]] = io >>= ToIterator.apply

    val runningIterator: Iterator[Element] = new DefaultRuntime {}.unsafeRun(iterator)

    def testIterator(prevElement: Element): Element = {
      val currentTime = System.nanoTime()
      val element     = runningIterator.next()
      assert(prevElement.n < element.n)
      assert(currentTime < element.time)
      element
    }

    Iterator.iterate(Element(n = 0, time = 0))(testIterator).take(200).foreach(_ => { Thread.sleep(1) })
  }

  test("<=>") {

    val in: List[Int] = (1 to 100).toList

    val test = for {
      _ <- UIO.unit
      stream1 = ZStream.fromIterator(UIO(in.toIterator))
      iterator <- ToIterator.apply(stream1)
      //stream2 = ZStream.fromIterator(UIO(iterator))
      //out <- stream2.runCollect
      out = iterator.toList
      _ <- ZIO.effect(assert(in == out))
    } yield {}

    new DefaultRuntime {}.unsafeRun(test)

  }

}
