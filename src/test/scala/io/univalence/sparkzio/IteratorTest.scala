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

    val iterator: ZIO[Clock, Nothing, Iterator[Nothing, Element]] =
      Iterator.unwrapManaged(io.toManaged_ >>= Iterator.fromStream)

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

    val test: ZManaged[Any, Nothing, Unit] = for {
      _ <- UIO.unit.toManaged_
      stream1 = ZStream.fromIterator(UIO(in.toIterator))
      iterator <- Iterator.fromStream(stream1)
      stream2 = ZStream.fromIterator(UIO(iterator))
      out <- stream2.runCollect.toManaged_
    } yield {
      assert(in == out)
    }

    new DefaultRuntime {}.unsafeRun(test.use_(ZIO.unit))
  }

  test("on Exit") {
    val test: ZManaged[Any, Nothing, Unit] = for {
      isOpen <- Ref.make(false).toManaged_
      stream = ZStream.managed(ZManaged.make(isOpen.update(_ => true))(_ => isOpen.set(false)))
      iterator <- Iterator.fromStream(stream)
    } yield {
      assert(iterator.toList == List(true))
    }

    new DefaultRuntime {}.unsafeRun(test.use_(ZIO.unit))
  }

}
