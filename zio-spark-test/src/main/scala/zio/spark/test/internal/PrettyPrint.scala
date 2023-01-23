package zio.spark.test.internal

import org.apache.spark.sql.Row
import zio.test.{ PrettyPrint => ZIOPrettyPrint}

object PrettyPrint {
  def apply(value: Any): String = value match {
    case row: Row => row.toString().drop(1).dropRight(1).split(",").mkString("Row(", ", ", ")")
    case _ => ZIOPrettyPrint(value)
  }
}
