package io.univalence.sparkzio

import org.scalatest.FunSuite
import zio.{ Ref, ZIO }
import zio.clock.Clock
import zio.stream.Stream

class ToInteratorTest extends FunSuite {

  test("iterator T") {

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
}
