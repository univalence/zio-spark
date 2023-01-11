package zio.spark.test

import zio.internal.stacktracer.SourceLocation
import zio.spark.test.internal.ColumnDescription
import zio.test.{ErrorMessage, TestArrow, TestResult, TestTrace}

sealed trait ExpectError {
  def explain: String

  def toTestResult(implicit sourceLocation: SourceLocation): TestResult =
    TestResult(
      TestArrow
        .make[Any, Boolean] { _ =>
          TestTrace.fail(ErrorMessage.custom(explain))
        }
        .withLocation(sourceLocation)
    )
}

object ExpectError {
  final case class WrongSchemaDefinition(missingColumns: List[ColumnDescription]) extends ExpectError {
    def add(missingColumn: ColumnDescription): WrongSchemaDefinition = copy(missingColumns = missingColumns :+ missingColumn)
    override def explain: String = {
      val theFollowingColumn =
        if (missingColumns.length == 1) "The following column description is missing"
        else "The following column descriptions are missing"

      s"""Can't match the given schema. $theFollowingColumn:
         |${missingColumns.map(_.toString).map(s => s" - $s").mkString("\n")}""".stripMargin
    }
  }
}
