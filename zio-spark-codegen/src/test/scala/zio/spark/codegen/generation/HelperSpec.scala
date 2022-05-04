package zio.spark.codegen.generation

import zio.spark.codegen.generation.template.Helper
import zio.test.{assertTrue, Spec, TestEnvironment, ZIOSpecDefault}

object HelperSpec extends ZIOSpecDefault {
  override def spec: Spec[TestEnvironment, Any] =
    suite("check helper generation") {
      test("helper's action should not use attemptBlocking but attempt") {
        assertTrue(!Helper.action("toto", typeParameters = List("T")).contains("attemptBlocking"))
      }
    }
}
