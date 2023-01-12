package zio.spark.test

import zio.spark.test.internal.ColumnDescription
import zio.test.{ErrorMessage, TestTrace}

sealed trait ExpectError {
  def explain: String

  def toTestTrace: TestTrace[Nothing] = TestTrace.fail(ErrorMessage.custom(explain))
}

object ExpectError {
  final case class WrongSchemaDefinition(missingColumns: List[ColumnDescription], isShortened: Boolean = false)
      extends ExpectError {
    def add(missingColumn: ColumnDescription): WrongSchemaDefinition =
      copy(missingColumns = missingColumns :+ missingColumn)

    def shorten: WrongSchemaDefinition = copy(isShortened = true)

    override def explain: String = {
      val theFollowingColumn =
        if (missingColumns.length == 1) "The following column description is missing"
        else "The following column descriptions are missing"

      val shortenString = if (isShortened) "\n - ..." else ""

      s"""Can't match the given schema. $theFollowingColumn:
         |${missingColumns.map(_.toString).map(s => s" - $s").mkString("\n")}$shortenString""".stripMargin
    }
  }

  final case class NoMatch[T](values: List[T], isShortened: Boolean = false) extends ExpectError {
    def add(value: T): NoMatch[T] = copy(values = values :+ value)

    def shorten: NoMatch[T] = copy(isShortened = true)

    override def explain: String = {
      val theFollowingValue =
        if (values.length == 1) "The following value has no match"
        else "The following values have no match"

      val shortenString = if (isShortened) "\n - ..." else ""

      s"""Can't find a matcher for all values. $theFollowingValue:
         |${values.map(_.toString).map(s => s" - $s").mkString("\n")}$shortenString""".stripMargin
    }
  }
}
