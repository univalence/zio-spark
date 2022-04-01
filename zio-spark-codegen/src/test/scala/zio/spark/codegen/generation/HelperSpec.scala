package zio.spark.codegen.generation

import zio.spark.codegen.generation.template.Helper
import zio.test.{assertTrue, DefaultRunnableSpec, TestEnvironment, ZSpec}

object HelperSpec extends DefaultRunnableSpec {
  override def spec: ZSpec[TestEnvironment, Any] =
    suite("check helper generation") {
      test("helper's action should not use attemptBlocking but attempt") {
        assertTrue(!Helper.action("toto", typeParameters = List("T")).contains("attemptBlocking"))
      }
    }
}
