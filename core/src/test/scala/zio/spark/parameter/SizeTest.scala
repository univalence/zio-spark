package zio.spark.parameter

import zio.test.{assert, Annotations, DefaultRunnableSpec, Live, Spec, TestFailure, TestSuccess}
import zio.test.Assertion.equalTo

object SizeTest extends DefaultRunnableSpec {

  val sizeSpec: Spec[Any, TestFailure[Throwable], TestSuccess] =
    suite("To string representation")({
      case class Conftest(text: String, input: Size, output: String)

      val conftests =
        List(
          Conftest("unlimited", unlimitedSize, "0"),
          Conftest("bytes", 2.bytes, "2b"),
          Conftest("bytes (alias)", 2.b, "2b"),
          Conftest("kibibytes", 3.kibibytes, "3kb"),
          Conftest("kibibytes (alias)", 3.kb, "3kb"),
          Conftest("mebibytes", 4.mebibytes, "4mb"),
          Conftest("mebibytes (alias)", 4.mb, "4mb"),
          Conftest("gibibytes", 5.gibibytes, "5gb"),
          Conftest("gibibytes (alias)", 5.gb, "5gb"),
          Conftest("tebibytes", 6.tebibytes, "6tb"),
          Conftest("tebibytes (alias)", 6.tb, "6tb"),
          Conftest("pebibytes", 7.pebibytes, "7pb"),
          Conftest("pebibytes (alias)", 7.pb, "7pb")
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
