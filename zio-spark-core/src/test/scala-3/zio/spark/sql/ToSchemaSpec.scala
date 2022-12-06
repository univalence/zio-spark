package zio.spark.sql

import org.apache.spark.sql.types._

import zio.Scope
import zio.spark.sql.ToSchema._
import zio.test._
import zio.test.Assertion._

object ToSchemaSpec extends ZIOSpecDefault {
  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("Convert case class into schema")(
      test("Case class with one string") {
        final case class Test(foo: String)

        val schema = StructType(Seq(StructField("foo", StringType, nullable = false)))
        assert(schema.treeString)(equalTo(summon[ToStructSchema[Test]].toSchema.treeString))
      },
      test("Case class with all basic types") {
        final case class Test(foo: String, bar: Boolean)

        val schema =
          StructType(
            Seq(
              StructField("foo", StringType, nullable  = false),
              StructField("bar", BooleanType, nullable = false)
            )
          )
        assert(summon[ToStructSchema[Test]].toSchema.treeString)(equalTo(schema.treeString))
      },
      test("Case class with one string and one optional boolean") {
        final case class Test(
            boolean:    Boolean,
            string:     String,
            short:      Short,
            integer:    Int,
            long:       Long,
            bigDecimal: BigDecimal,
            float:      Float,
            double:     Double,
            byte:       Byte,
            binary:     Array[Byte],
            timestamp:  java.sql.Timestamp,
            date:       java.sql.Date
        )

        val schema =
          StructType(
            Seq(
              StructField("boolean", BooleanType, nullable           = false),
              StructField("string", StringType, nullable             = false),
              StructField("short", ShortType, nullable               = false),
              StructField("integer", IntegerType, nullable           = false),
              StructField("long", LongType, nullable                 = false),
              StructField("bigDecimal", DecimalType(10, 0), nullable = false),
              StructField("float", FloatType, nullable               = false),
              StructField("double", DoubleType, nullable             = false),
              StructField("byte", ByteType, nullable                 = false),
              StructField("binary", BinaryType, nullable             = false),
              StructField("timestamp", TimestampType, nullable       = false),
              StructField("date", DateType, nullable                 = false)
            )
          )
        assert(summon[ToStructSchema[Test]].toSchema.treeString)(equalTo(schema.treeString))
      },
      test("Case class with a map") {
        final case class Test(foo: Map[String, Boolean])

        val schema =
          StructType(
            Seq(
              StructField("foo", DataTypes.createMapType(StringType, BooleanType), nullable = false)
            )
          )
        assert(summon[ToStructSchema[Test]].toSchema.treeString)(equalTo(schema.treeString))
      },
      test("Case class with a nested case class") {
        final case class Bar(baz: String)
        final case class Test(foo: Int, bar: Bar)

        val nestedSchema = StructType(Seq(StructField("baz", StringType, nullable = false)))
        val schema =
          StructType(
            Seq(
              StructField("foo", IntegerType, nullable  = false),
              StructField("bar", nestedSchema, nullable = false)
            )
          )

        assert(summon[ToStructSchema[Test]].toSchema.treeString)(equalTo(schema.treeString))
      },
      test("Case class with a nested nullable case class") {
        final case class Bar(baz: String)
        final case class Test(foo: Int, bar: Option[Bar])

        val nestedSchema = StructType(Seq(StructField("baz", StringType, nullable = false)))
        val schema =
          StructType(
            Seq(
              StructField("foo", IntegerType, nullable  = false),
              StructField("bar", nestedSchema, nullable = true)
            )
          )

        assert(summon[ToStructSchema[Test]].toSchema.treeString)(equalTo(schema.treeString))
      }
    )
}
