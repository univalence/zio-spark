package zio.spark

import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect._

import scala.reflect._

/**
 * A set of test to assert a feature compatibility between a Spark and a
 * Zio Spark structure.
 */
abstract class CompatibilityTestBetween[SparkStructure: ClassTag, ZioSparkStructure: ClassTag](
    allowedNewMethods: Seq[String],
    isImpure:          Boolean
) extends DefaultRunnableSpec {
  val sparkStructurePath: String    = classTag[SparkStructure].runtimeClass.getName
  val zioSparkStructurePath: String = classTag[ZioSparkStructure].runtimeClass.getName

  /** Get all methods of a particular class. */
  def getAllMethods[A: ClassTag]: Seq[String] =
    classTag[A].runtimeClass.getMethods
      .map(_.getName)
      .toSeq
      .filter(!_.contains("$"))
      .filter(!scalaDefaultMethods.contains(_))
      .distinct

  /** The default methods of any case class. */
  val scalaDefaultMethods: Seq[String] =
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
      "andThen"
    )

  /** Methods provided by the Impure class. */
  val impureMethods: Seq[String] =
    Seq(
      "ImpureBox",
      "attemptWithZIO",
      "attemptBlocking",
      "attempt"
    )

  def hasSameElementsPlusImpure(elements: Seq[String]): Assertion[Seq[String]] =
    hasSameElements(impureMethods ++ elements)

  override def spec: ZSpec[TestEnvironment, Any] =
    suite(s"Verifying compatibility between $sparkStructurePath and $zioSparkStructurePath")(
      test(s"$zioSparkStructurePath should implement all $sparkStructurePath methods") {
        val underlyingMethods: Seq[String]     = getAllMethods[SparkStructure]
        val methods: Seq[String]               = getAllMethods[ZioSparkStructure]
        val notImplementedMethods: Seq[String] = underlyingMethods.filter(!methods.contains(_))

        assert(notImplementedMethods)(isEmpty)
      } @@ failing,
      test(s"$zioSparkStructurePath should have limited new features compare to $sparkStructurePath") {
        val underlyingMethods: Seq[String]     = getAllMethods[SparkStructure]
        val methods: Seq[String]               = getAllMethods[ZioSparkStructure]
        val notImplementedMethods: Seq[String] = methods.filter(!underlyingMethods.contains(_))

        def assertion(elements: Seq[String]) =
          if (isImpure) hasSameElementsPlusImpure(elements) else hasSameElements(elements)

        assert(notImplementedMethods)(assertion(allowedNewMethods))
      }
    )
}
