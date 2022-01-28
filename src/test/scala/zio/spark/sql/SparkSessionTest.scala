package zio.spark.sql

import zio.test._
import zio.test.Assertion._

object SparkSessionTest extends DefaultRunnableSpec {

  import SparkSession.Builder._

  val masterConfigurationSpec: Spec[Any, TestFailure[Throwable], TestSuccess] =
    suite("MasterConfiguration")({
      case class MasterConfigurationTest(text: String, input: MasterConfiguration, output: String)

      val masterNodeConfiguration = MasterNodeConfiguration("url", 48)

      val conftests =
        List(
          MasterConfigurationTest("local all nodes", LocalAllNodes, "local[*]"),
          MasterConfigurationTest("local 4 nodes", Local(4), "local[4]"),
          MasterConfigurationTest("local 2 nodes with 8 failures", LocalWithFailures(2, 8), "local[2,8]"),
          MasterConfigurationTest("local all nodes with 6 failures", LocalAllNodesWithFailures(6), "local[*,6]"),
          MasterConfigurationTest("spark", Spark(List(masterNodeConfiguration)), "spark://url:48"),
          MasterConfigurationTest("mesos", Mesos(masterNodeConfiguration), "mesos://url:48"),
          MasterConfigurationTest("yarn", Yarn, "yarn")
        )

      val tests =
        conftests.map(conftest =>
          test(s"MasterConfiguration is converted into master correctly (${conftest.text})") {
            assert(masterConfigurationToMaster(conftest.input))(equalTo(conftest.output))
          }
        )

      tests
    }: _*)

  def spec: Spec[Annotations with Live, TestFailure[Any], TestSuccess] = masterConfigurationSpec

}
