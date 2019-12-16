package io.univalence.sparkzio

import org.scalatest.FunSuite
import zio.{ DefaultRuntime, Ref, UIO, ZIO, ZManaged }
import zio.clock.Clock
import zio.stream.{ Stream, ZStream }

class IteratorTest extends FunSuite {

  test("To Iterator should be lazy") {
    case class Element(n: Int, time: Long)

    val io: ZIO[Clock, Nothing, Stream[Nothing, Element]] = for {
      clock <- ZIO.environment[Clock]
      v     <- Ref.make(0)
      _     <- v.update(_ + 1).forever.fork
    } yield {
      Stream.repeatEffect(v.get.zipWith(clock.clock.nanoTime)(Element))
    }

    val iterator: ZIO[Clock, Nothing, Iterator[Nothing, Element]] = io >>= Iterator.fromStream

    val runningIterator: Iterator[Nothing, Element] = new DefaultRuntime {}.unsafeRun(iterator)

    def testIterator(prevElement: Element): Element = {
      val currentTime = System.nanoTime()
      val element     = runningIterator.next()
      assert(prevElement.n < element.n)
      assert(currentTime < element.time)
      element
    }

    scala.Iterator.iterate(Element(n = 0, time = 0))(testIterator).take(200).foreach(_ => { Thread.sleep(1) })
  }

  test("<=>") {

    val in: List[Int] = (1 to 100).toList

    val test = for {
      _ <- UIO.unit
      stream1 = ZStream.fromIterator(UIO(in.toIterator))
      iterator <- Iterator.fromStream(stream1)
      stream2 = ZStream.fromIterator(UIO(iterator))
      out <- stream2.runCollect
      _   <- ZIO.effect(assert(in == out))
    } yield {}

    new DefaultRuntime {}.unsafeRun(test)
  }

  test("on Exit") {
    val test = for {
      isOpen <- Ref.make(false)
      stream = ZStream.managed(ZManaged.make(isOpen.update(_ => true))(_ => isOpen.set(false)))
      iterator <- Iterator.fromStream(stream)
    } yield {
      assert(iterator.toList == List(true))
    }

    new DefaultRuntime {}.unsafeRun(test)
  }

}
