package zio.spark.parameter

import zio.test.{assert, Annotations, DefaultRunnableSpec, Live, Spec, TestFailure, TestSuccess}
import zio.test.Assertion.equalTo

object SizeTest extends DefaultRunnableSpec {

  val sizeSpec: Spec[Any, TestFailure[Throwable], TestSuccess] =
    suite("To string representation")({
      case class Conftest(text: String, input: Size, output: String)

      val conftests =
        List(
          Conftest("bytes", 1.byte, "1b"),
          Conftest("kibibytes", 2.kibibytes, "2kb"),
          Conftest("mebibytes", 3.mebibytes, "3mb"),
          Conftest("gibibytes", 4.gibibytes, "4gb"),
          Conftest("tebibytes", 5.tebibytes, "5tb"),
          Conftest("pebibytes", 6.pebibytes, "6pb")
        )

      val tests =
        conftests.map(conftest =>
          test(s"Size is converted into its string representation correctly (${conftest.text})") {
            assert(Size.sizeToString(conftest.input))(equalTo(conftest.output))
          }
        )

      tests
    }: _*)

  def spec: Spec[Annotations with Live, TestFailure[Any], TestSuccess] = sizeSpec

}
