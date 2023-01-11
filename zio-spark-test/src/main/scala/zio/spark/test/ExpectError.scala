package zio.spark.test

import zio.internal.stacktracer.SourceLocation
import zio.spark.test.internal.ColumnDescription
import zio.test.{ErrorMessage, TestTrace}

sealed trait ExpectError {
  def explain: String

  def toTestTrace(implicit sourceLocation: SourceLocation): TestTrace[Nothing] =
    TestTrace.fail(ErrorMessage.custom(explain))
}

object ExpectError {
  final case class WrongSchemaDefinition(missingColumns: List[ColumnDescription]) extends ExpectError {
    def add(missingColumn: ColumnDescription): WrongSchemaDefinition =
      copy(missingColumns = missingColumns :+ missingColumn)
    override def explain: String = {
      val theFollowingColumn =
        if (missingColumns.length == 1) "The following column description is missing"
        else "The following column descriptions are missing"

      s"""Can't match the given schema. $theFollowingColumn:
         |${missingColumns.map(_.toString).map(s => s" - $s").mkString("\n")}""".stripMargin
    }
  }

  object WrongSchemaDefinition {
    def apply(missingColumn: ColumnDescription): WrongSchemaDefinition = WrongSchemaDefinition(List(missingColumn))
  }

  final case class NoMatch[T](values: List[T]) extends ExpectError {
    def add(value: T): NoMatch[T] = copy(values = values :+ value)

    override def explain: String = {
      val theFollowingValue =
        if (values.length == 1) "The following value has no match"
        else "The following values have no match"

      s"""Can't find a matcher for all values. $theFollowingValue:
         |${values.map(_.toString).map(s => s" - $s").mkString("\n")}""".stripMargin
    }
  }

  object NoMatch {
    def apply[T](value: T): NoMatch[T] = NoMatch(List(value))
  }
}
