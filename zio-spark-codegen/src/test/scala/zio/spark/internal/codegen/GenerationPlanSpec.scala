package zio.spark.codegen

import zio.spark.codegen.GenerationPlan.Helper
import zio.test.*

object GenerationPlanSpec extends DefaultRunnableSpec {
  override def spec: ZSpec[TestEnvironment, Any] =
    suite("check attempt") {
      test("action helper") {
        assertTrue(!Helper.action("toto", typeParameters = List("T")).contains("attemptBlocking"))
      }
    }
}
