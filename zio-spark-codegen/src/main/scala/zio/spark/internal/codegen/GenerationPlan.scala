package zio.spark.internal.codegen

import sbt.*

import zio.spark.internal.codegen.MethodType.getMethodType
import zio.spark.internal.codegen.structure.Method

import scala.collection.immutable
import scala.meta.*
import scala.meta.contrib.AssociatedComments

sealed trait ScalaBinaryVersion { self =>
  override def toString: String =
    self match {
      case ScalaBinaryVersion.V2_11 => "2.11"
      case ScalaBinaryVersion.V2_12 => "2.12"
      case ScalaBinaryVersion.V2_13 => "2.13"
    }
}
object ScalaBinaryVersion {
  case object V2_11 extends ScalaBinaryVersion
  case object V2_12 extends ScalaBinaryVersion
  case object V2_13 extends ScalaBinaryVersion
}

case class GenerationPlan(module: String, path: String, source: meta.Source, scalaBinaryVersion: ScalaBinaryVersion) {
  import GenerationPlan.*

  /** @return the plan type according to the path. */
  val planType: PlanType =
    path match {
      case "org/apache/spark/rdd/RDD.scala"     => GenerationPlan.RDDPlan
      case "org/apache/spark/sql/Dataset.scala" => GenerationPlan.DatasetPlan
    }

  /** @return the name of the source file. */
  def name: String = path.replace(".scala", "").split('/').last

  /** @return the package name using the file path. */
  def pkg: String = path.replace(".scala", "").replace('/', '.')

  /** @return the path of the generated base file. */
  def sparkZioPath: String = path.replace("org/apache/spark", "zio/spark")

  /**
   * Retrieves all function's names from a file.
   *
   * @param file
   *   The file to retrieve functions from
   * @return
   *   The names of the functions
   */
  def functionsFromFile(file: File): Set[String] = {
    val source: Source = IO.read(file).parse[Source].get
    val template       = getTemplateFromSource(source)
    collectFunctionsFromTemplate(template).map(_.name.value).toSet

  }

  /**
   * Returns the final methods resulting from the fusion of the
   * generated functions and the handmade functions.
   *
   * @param scalaSource
   *   The sbt path of the Scala source
   * @return
   *   The set of method's names
   */
  def getFinalClassMethods(scalaSource: File): Set[String] = {
    val baseClassFunctions = functionsFromFile(scalaSource / sparkZioPath)

    planType match {
      case GenerationPlan.RDDPlan => baseClassFunctions
      case GenerationPlan.DatasetPlan =>
        val baseFile: File = new File(scalaSource.getPath + "-" + scalaBinaryVersion)
        val file: File     = baseFile / "zio" / "spark" / "sql" / "ExtraDatasetFeature.scala"
        baseClassFunctions ++ functionsFromFile(file)
    }
  }

  def checkMods(mods: List[Mod]): Boolean =
    !mods.exists {
      case mod"@DeveloperApi" => true
      case mod"private[$_]"   => true
      case mod"protected[$_]" => true
      case _                  => false
    }

  def collectFunctionsFromTemplate(template: Template): immutable.Seq[Defn.Def] =
    template.stats.collect {
      case d: Defn.Def if checkMods(d.mods) => d
      // case d: Decl.Def if checkMods(d.mods) => ??? // only compute is declared
    }

  def getTemplateFromSource(source: Source): Template =
    source.children
      .flatMap(_.children)
      .collectFirst {
        case c: Defn.Class if c.name.toString == "RDD"                 => c.templ
        case c: Defn.Class if c.name.toString == "Dataset"             => c.templ
        case c: Defn.Class if c.name.toString == "ExtraDatasetFeature" => c.templ
      }
      .get

  /** @return the methods of the spark source file. */
  lazy val methods: Seq[Method] = {
    val fileSource = source

    val template                     = getTemplateFromSource(fileSource)
    val allMethods                   = collectFunctionsFromTemplate(template)
    val comments: AssociatedComments = contrib.AssociatedComments(template)

    allMethods.map(m => Method.fromScalaMeta(m, comments, path.replace('/', '.').replace(".scala", ""), scalaBinaryVersion))
  }

  lazy val methodsWithTypes: Map[MethodType, Seq[Method]] =
    methods
      .filterNot(_.fullName.contains("$"))
      .filterNot(_.fullName.contains("java.lang.Object"))
      .filterNot(_.fullName.contains("scala.Any"))
      .filterNot(_.fullName.contains("<init>"))
      .groupBy(getMethodType)

  /** @return the implicits needed for each plans. */
  def baseImplicits: String = {
    val encoder: String = planType.fold("", "(implicit enc: Encoder[Seq[U]])")

    val rddImplicits =
      s"""private implicit def arrayToSeq2[U](x: Underlying$name[Array[U]])$encoder: Underlying$name[Seq[U]] = x.map(_.toIndexedSeq)
         |@inline private def noOrdering[U]: Ordering[U] = null""".stripMargin

    val datasetImplicits =
      s"""private implicit def iteratorConversion[U](iterator: java.util.Iterator[U]):Iterator[U] = iterator.asScala""".stripMargin

    val defaultImplicits =
      s"""private implicit def lift[U](x:Underlying$name[U]):$name[U] = $name(x)
         |private implicit def escape[U](x:$name[U]):Underlying$name[U] = x.underlying$name.succeedNow(v => v)""".stripMargin

    val implicits = defaultImplicits + "\n" + planType.fold(rddImplicits, datasetImplicits)

    s"""  // scalafix:off
       |$implicits
       |  // scalafix:on
       |""".stripMargin
  }

  /** @return the imports needed for each plans. */
  def imports: String = {
    val rddCommonImports =
      """import org.apache.hadoop.io.compress.CompressionCodec
        |import org.apache.spark.{Dependency, Partition, Partitioner, TaskContext}
        |import org.apache.spark.partial.{BoundedDouble, PartialResult}
        |import org.apache.spark.rdd.{PartitionCoalescer, RDD => UnderlyingRDD}
        |import org.apache.spark.storage.StorageLevel
        |
        |import zio.Task
        |import zio.spark.internal.Impure
        |import zio.spark.internal.Impure.ImpureBox
        |import zio.spark.rdd.RDD
        |
        |import scala.collection.Map
        |import scala.io.Codec
        |import scala.reflect._
        |""".stripMargin

    val datasetCommonImports =
      """import org.apache.spark.sql.{Column, Dataset => UnderlyingDataset, Encoder, Row, TypedColumn}
        |import org.apache.spark.sql.types.StructType
        |import org.apache.spark.storage.StorageLevel
        |
        |import zio.Task
        |import zio.spark.internal.Impure
        |import zio.spark.internal.Impure.ImpureBox
        |import zio.spark.sql.{DataFrame, Dataset, TryAnalysis}
        |
        |import scala.reflect.runtime.universe.TypeTag
        |""".stripMargin

    val rddSpecificImports =
      scalaBinaryVersion match {
        case ScalaBinaryVersion.V2_13 =>
          s"""import org.apache.spark.rdd.RDDBarrier
             |import org.apache.spark.resource.ResourceProfile
             |""".stripMargin
        case ScalaBinaryVersion.V2_12 =>
          s"""import org.apache.spark.rdd.RDDBarrier
             |""".stripMargin
        case _ => ""
      }

    val datasetSpecificImports =
      scalaBinaryVersion match {
        case ScalaBinaryVersion.V2_13 =>
          s"""import scala.jdk.CollectionConverters._
             |""".stripMargin
        case _ =>
          s"""import scala.collection.JavaConverters._
             |""".stripMargin
      }

    val rddImports     = rddCommonImports + rddSpecificImports
    val datasetImports = datasetCommonImports + datasetSpecificImports

    planType.fold(rddImports, datasetImports)
  }

  /** @return the helper functions needed for each plans. */
  def helpers: String = {
    val defaultHelpers =
      s"""/** Applies an action to the underlying $name. */
         |def action[U](f: Underlying$name[T] => U): Task[U] = attemptBlocking(f)
         |
         |/** Applies a transformation to the underlying $name. */
         |def transformation[U](f: Underlying$name[T] => Underlying$name[U]): $name[U] = succeedNow(f.andThen(x => $name(x)))""".stripMargin

    val datasetHelpers =
      """/**
        | * Applies a transformation to the underlying dataset, it is used for
        | * transformations that can fail due to an AnalysisException.
        | */
        |def transformationWithAnalysis[U](f: UnderlyingDataset[T] => UnderlyingDataset[U]): TryAnalysis[Dataset[U]] =
        |  TryAnalysis(transformation(f))
        |
        |/**
        | * Wraps a function into a TryAnalysis.
        | */
        |def withAnalysis[U](f: UnderlyingDataset[T] => U): TryAnalysis[U] =
        |  TryAnalysis(succeedNow(f))
        |""".stripMargin

    defaultHelpers + "\n\n" + planType.fold("", datasetHelpers)
  }

  val suppressWarnings: String = {
    val rddSuppressWarnings = "@SuppressWarnings(Array(\"scalafix:DisableSyntax.defaultArgs\", \"scalafix:DisableSyntax.null\"))"

    planType.fold(rddSuppressWarnings, "")
  }
}

object GenerationPlan {
  sealed trait PlanType {
    def fold[C](rdd: => C, dataset: => C): C =
      this match {
        case RDDPlan     => rdd
        case DatasetPlan => dataset
      }
  }
  case object RDDPlan     extends PlanType
  case object DatasetPlan extends PlanType

  /**
   * Retrieve the generation plan according to the module name, the file
   * name and the classpath.
   *
   * @param module
   *   The name of the module e.g. "spark-core"
   * @param file
   *   The path of the source file e.g. "org/apache/spark/rdd/RDD.scala"
   * @param classpath
   *   The classpath
   * @return
   *   The generation plan
   */
  private def get(
      module: String,
      file: String,
      classpath: GetSources.Classpath,
      scalaBinaryVersion: ScalaBinaryVersion
  ): zio.Task[GenerationPlan] =
    GetSources
      .getSource(module, file)(classpath)
      .map(source => GenerationPlan(module, file, source, scalaBinaryVersion))

  def rddPlan(classpath: GetSources.Classpath, scalaBinaryVersion: ScalaBinaryVersion): zio.Task[GenerationPlan] =
    get("spark-core", "org/apache/spark/rdd/RDD.scala", classpath, scalaBinaryVersion)

  def datasetPlan(classpath: GetSources.Classpath, scalaBinaryVersion: ScalaBinaryVersion): zio.Task[GenerationPlan] =
    get("spark-sql", "org/apache/spark/sql/Dataset.scala", classpath, scalaBinaryVersion)

}
