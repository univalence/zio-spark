package zio.spark.helper

import zio.test._

final case class Conftest[T](text: String, input: T, output: String)

abstract class ADTTestFor[T](name: String, conftests: List[Conftest[T]]) extends ZIOSpecDefault {
  def spec: Spec[Annotations with Live, TestFailure[Any]] =
    suite(s"$name ADT spec")({
      val tests =
        conftests.map(conftest =>
          test(s"$name is converted into its string representation correctly (${conftest.text})") {
            assertTrue(conftest.input.toString == conftest.output)
          }
        )
      tests
    }: _*)
}
