package zio.spark.helper

import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect._

import scala.reflect._

/**
 * A set of test to assert a feature compatibility between a Spark and a
 * Zio Spark structure.
 */
abstract class CompatibilityTestBetween[SparkStructure: ClassTag, ZioSparkStructure: ClassTag](
    allowedNewMethods: Seq[String]
) extends ZIOSpecDefault {

  // scalafix:off
  lazy val sparkStructurePath: String    = classTag[SparkStructure].runtimeClass.getName
  lazy val zioSparkStructurePath: String = classTag[ZioSparkStructure].runtimeClass.getName
  // scalafix:on

  /** Get all methods of a particular class. */
  def getAllMethods[A: ClassTag]: Seq[String] =
    classTag[A].runtimeClass.getMethods
      .map(_.getName)
      .toSeq
      .filter(!_.contains("$"))
      .filter(!scalaDefaultMethods.contains(_))
      .filter(!_.startsWith("_"))
      .distinct

  /** The default methods of any case class. */
  def scalaDefaultMethods: Seq[String] =
    Seq(
      "apply",
      "copy",
      "productElementNames",
      "productPrefix",
      "productArity",
      "productElement",
      "productIterator",
      "productElementName",
      "unapply",
      "canEqual",
      "compose",
      "andThen",
      "fromProduct"
    )

  override def spec: Spec[TestEnvironment, Any] =
    suite(s"Verifying compatibility between $sparkStructurePath and $zioSparkStructurePath")(
      test(s"$zioSparkStructurePath should implement all $sparkStructurePath methods") {
        val underlyingMethods: Seq[String]     = getAllMethods[SparkStructure]
        val methods: Seq[String]               = getAllMethods[ZioSparkStructure]
        val notImplementedMethods: Seq[String] = underlyingMethods.filter(!methods.contains(_))

        assertTrue(notImplementedMethods.isEmpty)
      } @@ failing,
      test(s"$zioSparkStructurePath should have limited new features compare to $sparkStructurePath") {
        val underlyingMethods: Seq[String]     = getAllMethods[SparkStructure]
        val methods: Seq[String]               = getAllMethods[ZioSparkStructure]
        val notImplementedMethods: Seq[String] = methods.filter(!underlyingMethods.contains(_))

        def assertion(elements: Seq[String]) = hasSameElements(elements)

        assert(notImplementedMethods)(assertion(allowedNewMethods))
      }
    )
}
