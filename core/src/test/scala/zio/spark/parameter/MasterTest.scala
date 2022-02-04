package zio.spark.parameter

import zio.test.{assert, Annotations, DefaultRunnableSpec, Live, Spec, TestFailure, TestSuccess}
import zio.test.Assertion.equalTo

object MasterTest extends DefaultRunnableSpec {
  val masterSpec: Spec[Any, TestFailure[Throwable], TestSuccess] =
    suite("To string representation")({
      case class Conftest(text: String, input: Master, output: String)

      val masterNodeConfiguration = url("localhost", 48)

      val conftests =
        List(
          Conftest("local all nodes", localAllNodes, "local[*]"),
          Conftest("local 4 nodes", local(4), "local[4]"),
          Conftest("local 2 nodes with 8 failures", localWithFailures(2, 8), "local[2,8]"),
          Conftest("local all nodes with 6 failures", localAllNodesWithFailures(6), "local[*,6]"),
          Conftest("spark", spark(List(masterNodeConfiguration)), "spark://localhost:48"),
          Conftest("mesos", mesos(masterNodeConfiguration), "mesos://localhost:48"),
          Conftest("yarn", yarn, "yarn")
        )

      val tests =
        conftests.map(conftest =>
          test(s"Master is converted into its string representation correctly (${conftest.text})") {
            assert(Master.masterToString(conftest.input))(equalTo(conftest.output))
          }
        )

      tests
    }: _*)

  def spec: Spec[Annotations with Live, TestFailure[Any], TestSuccess] = masterSpec

}
