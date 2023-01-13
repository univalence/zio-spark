package zio.spark.test.internal

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import zio._
import zio.spark.test.internal.ValueMatcher.GlobalValueMatcher._
import zio.spark.test.internal.ValueMatcher.PositionalValueMatcher._
import zio.test._

object ValueMatcherSpec extends ZIOSpecDefault {
  override def spec: Spec[TestEnvironment with Scope, Any] = {
    val defaultSchema = StructType(Seq(StructField("value", IntegerType)))

    suite("ValueMatcher spec")(
      test("Value should works for type T with good value") {
        val matcher = Value(10)

        assertTrue(matcher.process(10, defaultSchema))
      },
      test("Value should works for row with good value") {
        val matcher = Value(Row("John"))

        assertTrue(matcher.process(Row("John"), defaultSchema))
      },
      test("Value should fail for type T with wrong value but good type") {
        val matcher = Value(10)

        assertTrue(matcher.process(9, defaultSchema) == false)
      },
      test("Value should fail for type T with wrong value but wrong type") {
        val matcher = Value(10)

        assertTrue(matcher.process("10", defaultSchema) == false)
      },
      test("KeyValue should works for type T with value as key") {
        val matcher = KeyValue("value", 10)

        assertTrue(matcher.process(10, defaultSchema))
      },
      test("KeyValue should works for type rows with correct key value") {
        val matcher = KeyValue("name", "John")
        val fields  = Seq(StructField("name", StringType))
        val schema  = StructType(fields)
        val row     = Row("John")

        assertTrue(matcher.process(row, schema))
      }
    )
  }
}
