package zio.spark.helper

import zio.test._
import zio.test.Assertion._

final case class Conftest[T](text: String, input: T, output: String)

abstract class ADTTestFor[T](name: String, conftests: List[Conftest[T]]) extends DefaultRunnableSpec {
  def spec: Spec[Annotations with Live, TestFailure[Any], TestSuccess] =
    suite(s"$name ADT spec")({
      val tests =
        conftests.map(conftest =>
          test(s"$name is converted into its string representation correctly (${conftest.text})") {
            assert(conftest.input.toString)(equalTo(conftest.output))
          }
        )
      tests
    }: _*)
}
