package zio.spark.test.internal

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import zio.Scope
import zio.spark.test._
import zio.spark.test.ExpectError.WrongSchemaDefinition
import zio.test._

object SchemaMatcherSpec extends ZIOSpecDefault {
  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("SchemaMatcher spec")(
      test("SchemaMatcher should generate a mapping from a schema") {
        val matcher = schema("name", "age", "height")
        val structType =
          StructType(
            Seq(
              StructField("height", IntegerType),
              StructField("age", IntegerType),
              StructField("name", StringType)
            )
          )
        val expected = Map(0 -> 2, 1 -> 1, 2 -> 0)

        assertTrue(matcher.definitionToSchemaIndex(structType) == Right(expected))
      },
      test("SchemaMatcher should generate a partial mapping from a schema") {
        val matcher = schema("name", "age")
        val structType =
          StructType(
            Seq(
              StructField("height", IntegerType),
              StructField("age", IntegerType),
              StructField("name", StringType)
            )
          )
        val expected = Map(0 -> 2, 1 -> 1)

        assertTrue(matcher.definitionToSchemaIndex(structType) == Right(expected))
      },
      test("SchemaMatcher should fail if we need an unknown column name") {
        val matcher = schema("name", "age", "unknown")
        val structType =
          StructType(
            Seq(
              StructField("height", IntegerType),
              StructField("age", IntegerType),
              StructField("name", StringType)
            )
          )
        val expected = WrongSchemaDefinition(List(ColumnDescription("unknown", None)))

        assertTrue(matcher.definitionToSchemaIndex(structType) == Left(expected))
      }
    )
}
