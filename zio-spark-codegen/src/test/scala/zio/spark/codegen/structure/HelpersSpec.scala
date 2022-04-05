package zio.spark.codegen.structure

import zio.spark.codegen.structure.Helpers.mergeSources
import zio.test.{assertTrue, DefaultRunnableSpec, TestEnvironment, ZSpec}
import zio.test.TestAspect.ignore

import scala.meta.*

object HelpersSpec extends DefaultRunnableSpec {
  override def spec: ZSpec[TestEnvironment, Any] =
    suite("Merge two sources together")(
      test("Merge case classes together") {
        val baseCode =
          s"""class Test { self =>
             |  def hello: String = "Hello World"
             |}""".stripMargin

        val specificCode =
          s"""class TestSpecific(self: Test) {
             |  def goodbye: String = "Good Bye"
             |}""".stripMargin

        val expectedCode =
          s"""class Test { self =>
             |  def hello: String = "Hello World"
             |  def goodbye: String = "Good Bye"
             |}""".stripMargin

        val baseSource     = baseCode.parse[Source].get
        val specificSource = specificCode.parse[Source].get
        val expectedSource = mergeSources(baseSource, specificSource)

        assertTrue(expectedCode == expectedSource)
      },
      test("Merge object together") {
        val baseCode =
          s"""object Test {
             |  def hello: String = "Hello World"
             |}""".stripMargin

        val specificCode =
          s"""object TestSpecific {
             |  def goodbye: String = "Good Bye"
             |}""".stripMargin

        val expectedCode =
          s"""object Test {
             |  def hello: String = "Hello World"
             |  def goodbye: String = "Good Bye"
             |}""".stripMargin

        val baseSource     = baseCode.parse[Source].get
        val specificSource = specificCode.parse[Source].get
        val expectedSource = mergeSources(baseSource, specificSource)

        assertTrue(expectedCode == expectedSource)
      },
      test("Merge comments together") {
        val baseCode =
          s"""object Test {
             |  def hello: String = "Hello World"
             |}""".stripMargin

        val specificCode =
          s"""object TestSpecific {
             |  def goodbye: String = "Good Bye"
             |}""".stripMargin

        val expectedCode =
          s"""object Test {
             |  def hello: String = "Hello World"
             |  def goodbye: String = "Good Bye"
             |}""".stripMargin

        val baseSource     = baseCode.parse[Source].get
        val specificSource = specificCode.parse[Source].get
        val expectedSource = mergeSources(baseSource, specificSource)

        assertTrue(expectedCode == expectedSource.toString())
      } @@ ignore
    )
}
