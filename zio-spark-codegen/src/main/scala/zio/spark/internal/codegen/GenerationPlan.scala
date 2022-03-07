package zio.spark.internal.codegen

import sbt.*

import zio.spark.internal.codegen.GenerationPlan.PlanType
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

case class GenerationPlan(planType: PlanType, source: meta.Source, scalaBinaryVersion: ScalaBinaryVersion) {
  import GenerationPlan.*

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
    val baseClassFunctions = functionsFromFile(scalaSource / planType.sparkZioPath)

    planType match {
      case GenerationPlan.RDDPlan => baseClassFunctions
      case GenerationPlan.DatasetPlan =>
        val baseFile: File = new File(scalaSource.getPath + "-" + scalaBinaryVersion)
        val file: File     = baseFile / "zio" / "spark" / "sql" / "ExtraDatasetFeature.scala"
        baseClassFunctions ++ functionsFromFile(file)
      case GenerationPlan.DataFrameNaFunctionsPlan => baseClassFunctions
    }
  }

  /** @return the methods of the spark source file. */
  lazy val methods: Seq[Method] = {
    val fileSource = source

    val template                     = getTemplateFromSource(fileSource)
    val scalametaMethods             = collectFunctionsFromTemplate(template)
    val comments: AssociatedComments = contrib.AssociatedComments(template)
    val allMethods                   = scalametaMethods.map(m => Method.fromScalaMeta(m, comments, planType.pkg, scalaBinaryVersion))

    allMethods
      .filterNot(_.fullName.contains("$"))
      .filterNot(_.fullName.contains("java.lang.Object"))
      .filterNot(_.fullName.contains("scala.Any"))
      .filterNot(_.fullName.contains("<init>"))
      .filterNot(_.calls.flatMap(_.parameters).map(_.signature).exists(_.contains("java")))  // Java specific implementation
      .filterNot(_.calls.flatMap(_.parameters).map(_.signature).exists(_.contains("Array"))) // Java specific implementation
  }

  lazy val methodsWithTypes: Map[MethodType, Seq[Method]] = methods.groupBy(getMethodType(_, planType))
}

object GenerationPlan {
  sealed abstract class PlanType(module: String, path: String) { self =>
    final def name: String = path.replace(".scala", "").split('/').last

    final def outputName: String =
      fold {
        case RDDPlan | DatasetPlan => s"Base$name"
        case _                     => name
      }

    final def pkg: String = path.replace(".scala", "").replace('/', '.')

    final def sparkZioPath: String = path.replace("org/apache/spark", "zio/spark")

    final def isAbstractClass: Boolean =
      fold {
        case RDDPlan | DatasetPlan => true
        case _                     => false
      }

    final def hasTypeParameter: Boolean =
      fold {
        case RDDPlan | DatasetPlan => true
        case _                     => false
      }

    final def definition: String = {
      val tparam = if (hasTypeParameter) "[T]" else ""
      val mod    = if (isAbstractClass) "abstract" else "case"

      val className      = s"$outputName$tparam"
      val underlyingName = s"Underlying$name$tparam"

      s"$mod class $className(underlying$name: ImpureBox[$underlyingName]) extends Impure[$underlyingName](underlying$name)"
    }

    final def implicits: String = {
      val defaultImplicits =
        s"""private implicit def lift[U](x:Underlying$name[U]):$name[U] = $name(x)
           |private implicit def escape[U](x:$name[U]):Underlying$name[U] = x.underlying$name.succeedNow(v => v)""".stripMargin

      val allImplicits =
        fold {
          case RDDPlan =>
            s"""$defaultImplicits
               |
               |private implicit def arrayToSeq2[U](x: Underlying$name[Array[U]]): Underlying$name[Seq[U]] = x.map(_.toIndexedSeq)
               |@inline private def noOrdering[U]: Ordering[U] = null""".stripMargin
          case DatasetPlan =>
            s"""$defaultImplicits
               |
               |private implicit def iteratorConversion[U](iterator: java.util.Iterator[U]):Iterator[U] = iterator.asScala
               |private implicit def liftDataFrameNaFunctions[U](x: UnderlyingDataFrameNaFunctions): DataFrameNaFunctions = DataFrameNaFunctions(ImpureBox(x))""".stripMargin
          case _ => ""
        }

      if (allImplicits.nonEmpty) {
        s"""  // scalafix:off
           |$allImplicits
           |  // scalafix:on
           |""".stripMargin
      } else allImplicits
    }

    final def helpers: String = {
      val defaultHelpers =
        s"""/** Applies an action to the underlying $name. */
           |def action[U](f: Underlying$name[T] => U): Task[U] = attemptBlocking(f)
           |
           |/** Applies a transformation to the underlying $name. */
           |def transformation[U](f: Underlying$name[T] => Underlying$name[U]): $name[U] = succeedNow(f.andThen(x => $name(x)))""".stripMargin

      fold {
        case RDDPlan => defaultHelpers
        case DatasetPlan =>
          s"""
             |$defaultHelpers
             |
             |/**
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
        case DataFrameNaFunctionsPlan =>
          """/**
            | * Applies a transformation to the underlying DataFrameNaFunctions.
            | */
            |def transformation(f: UnderlyingDataFrameNaFunctions => UnderlyingDataFrame): DataFrame =
            |  succeedNow(f.andThen(x => Dataset(x)))
            |
            |/**
            | * Applies a transformation to the underlying DataFrameNaFunctions, it is used for
            | * transformations that can fail due to an AnalysisException.
            | */
            |def transformationWithAnalysis(f: UnderlyingDataFrameNaFunctions => UnderlyingDataFrame): TryAnalysis[DataFrame] =
            |  TryAnalysis(transformation(f))
            |""".stripMargin
      }
    }

    final def imports(scalaBinaryVersion: ScalaBinaryVersion): String =
      fold {
        case RDDPlan =>
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

          s"""$rddCommonImports
             |$rddSpecificImports""".stripMargin
        case DatasetPlan =>
          val datasetCommonImports =
            """import org.apache.spark.sql.{Column, Dataset => UnderlyingDataset, DataFrameNaFunctions => UnderlyingDataFrameNaFunctions, Encoder, Row, TypedColumn}
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

          val datasetSpecificImports =
            scalaBinaryVersion match {
              case ScalaBinaryVersion.V2_13 =>
                s"""import scala.jdk.CollectionConverters._
                   |""".stripMargin
              case _ =>
                s"""import scala.collection.JavaConverters._
                   |""".stripMargin
            }

          s"""$datasetCommonImports
             |$datasetSpecificImports""".stripMargin
        case DataFrameNaFunctionsPlan =>
          """import org.apache.spark.sql.{DataFrame => UnderlyingDataFrame, DataFrameNaFunctions => UnderlyingDataFrameNaFunctions}
            |import zio.spark.internal.Impure
            |import zio.spark.internal.Impure.ImpureBox
            |import zio.spark.sql.{DataFrame, Dataset, TryAnalysis}
            |""".stripMargin
      }

    final def suppressWarnings: String =
      fold {
        case RDDPlan => "@SuppressWarnings(Array(\"scalafix:DisableSyntax.defaultArgs\", \"scalafix:DisableSyntax.null\"))"
        case _       => ""
      }

    final def sourceCode(body: String, scalaBinaryVersion: ScalaBinaryVersion): String =
      s"""/**
         | * /!\\ Warning /!\\
         | *
         | * This file is generated using zio-spark-codegen, you should not edit
         | * this file directly.
         | */
         |
         |package zio.spark.internal.codegen
         |
         |${imports(scalaBinaryVersion)}
         |
         |$suppressWarnings
         |$definition {
         |  import underlying$name._
         |
         |$implicits
         |
         |$helpers
         |
         |$body
         |
         |}
         |""".stripMargin

    final def getGenerationPlan(
        classpath: GetSources.Classpath,
        scalaBinaryVersion: ScalaBinaryVersion
    ): zio.Task[GenerationPlan] =
      GetSources
        .getSource(module, path)(classpath)
        .map(source => GenerationPlan(self, source, scalaBinaryVersion))

    @inline final def fold[C](planType: PlanType => C): C = planType(this)

    final def fold[C](rdd: => C, dataset: => C, dataFrameNa: => C): C =
      this match {
        case RDDPlan                  => rdd
        case DatasetPlan              => dataset
        case DataFrameNaFunctionsPlan => dataFrameNa
      }
  }

  case object RDDPlan                  extends PlanType("spark-core", "org/apache/spark/rdd/RDD.scala")
  case object DatasetPlan              extends PlanType("spark-sql", "org/apache/spark/sql/Dataset.scala")
  case object DataFrameNaFunctionsPlan extends PlanType("spark-sql", "org/apache/spark/sql/DataFrameNaFunctions.scala")

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
      .collectFirst { case c: Defn.Class =>
        c.templ
      }
      .get
}
