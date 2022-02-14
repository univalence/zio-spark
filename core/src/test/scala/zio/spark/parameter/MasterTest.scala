package zio.spark.parameter

import zio.spark.helper._
import zio.spark.parameter.Master.MasterNodeConfiguration

object MasterTest
    extends ADTTestFor[Master](
      name = "Master",
      conftests = {
        val masterNodeConfiguration: MasterNodeConfiguration = url("localhost", 48)

        List(
          Conftest("local all nodes", localAllNodes, "local[*]"),
          Conftest("local 4 nodes", local(4), "local[4]"),
          Conftest("local 2 nodes with 8 failures", localWithFailures(2, 8), "local[2,8]"),
          Conftest("local all nodes with 6 failures", localAllNodesWithFailures(6), "local[*,6]"),
          Conftest("spark", spark(List(masterNodeConfiguration)), "spark://localhost:48"),
          Conftest("mesos", mesos(masterNodeConfiguration), "mesos://localhost:48"),
          Conftest("yarn", yarn, "yarn")
        )
      }
    )
