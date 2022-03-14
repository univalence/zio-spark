package zio.spark.internal.codegen

import sbt.*

import zio.spark.internal.codegen.GenerationPlan.PlanType
import zio.spark.internal.codegen.ScalaBinaryVersion.versioned
import zio.spark.internal.codegen.structure.{Method, TemplateWithComments}

import scala.collection.immutable
import scala.meta.*
import scala.util.Try

sealed trait ScalaBinaryVersion {
  self =>
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

  def versioned(file: File, scalaVersion: ScalaBinaryVersion): File = new File(file.getPath + "-" + scalaVersion)
}

case class GenerationPlan(
    planType:           PlanType,
    source:             meta.Source,
    overlay:            Option[meta.Source],
    overlaySpecific:    Option[meta.Source],
    scalaBinaryVersion: ScalaBinaryVersion
) {

  import GenerationPlan.*

  /**
   * Retrieves all function's from a file.
   *
   * @param source
   *   The source to retrieve functions from
   * @return
   *   The sequence of functions
   */
  def functionsFromFile(source: Source, filterOverlay: Boolean): Seq[Method] = {
    val template: TemplateWithComments =
      if (filterOverlay) getTemplateFromSourceOverlay(source)
      else getTemplateFromSource(source)

    val scalametaMethods = collectFunctionsFromTemplate(template)
    scalametaMethods.map(m => Method.fromScalaMeta(m, template.comments, planType, scalaBinaryVersion))
  }

  /** @return the methods of the spark source file. */
  lazy val sourceMethods: Seq[Method] =
    functionsFromFile(source, false)
      .filterNot(_.fullName.contains("$"))
      .filterNot(_.fullName.contains("java.lang.Object"))
      .filterNot(_.fullName.contains("scala.Any"))
      .filterNot(_.fullName.contains("<init>"))
      .filterNot(_.anyParameters.map(_.signature).exists(_.contains("ju.")))   // Java specific implementation
      .filterNot(_.anyParameters.map(_.signature).exists(_.contains("jl.")))   // Java specific implementation
      .filterNot(_.anyParameters.map(_.signature).exists(_.contains("java")))  // Java specific implementation
      .filterNot(_.anyParameters.map(_.signature).exists(_.contains("Array"))) // Java specific implementation

  lazy val overlayMethods: Seq[Method] =
    overlay.map(functionsFromFile(_, true)).getOrElse(Seq.empty) ++ overlaySpecific
      .map(functionsFromFile(_, true))
      .getOrElse(Seq.empty)

  lazy val methods: Seq[Method] = sourceMethods ++ overlayMethods
}

object GenerationPlan {
  sealed abstract class PlanType(module: String, path: String) {
    self =>
    final def name: String = path.replace(".scala", "").split('/').last

    final def pkg: String = path.replace(".scala", "").replace('/', '.')

    final def zioSparkPath: String = path.replace("org/apache/spark", "zio/spark")

    final def zioSparkPackage: String = zioSparkPath.split("/").dropRight(1).mkString(".")

    final def hasTypeParameter: Boolean =
      fold {
        case RDDPlan | DatasetPlan => true
        case _                     => false
      }

    final def tparam: String = if (hasTypeParameter) "[T]" else ""

    final def definition: String = {
      val className      = s"$name$tparam"
      val underlyingName = s"Underlying$name$tparam"

      s"final case class $className(underlying$name: $underlyingName)"
    }

    final def implicits: String = {
      val defaultImplicits =
        s"""private implicit def lift[U](x:Underlying$name[U]):$name[U] = $name(x)
           |private implicit def escape[U](x:$name[U]):Underlying$name[U] = x.underlying$name""".stripMargin

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
               |private implicit def liftDataFrameNaFunctions[U](x: UnderlyingDataFrameNaFunctions): DataFrameNaFunctions = DataFrameNaFunctions(x)
               |private implicit def liftDataFrameStatFunctions[U](x: UnderlyingDataFrameStatFunctions): DataFrameStatFunctions = DataFrameStatFunctions(x)""".stripMargin
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
      // NOTE : action need to stay an attempt, and not an attemptBlocked for the moment.
      // 1. The ZIO Scheduler will catch up and treat it as if it's an attemptBlocked
      // 2. It's necessary for "makeItCancellable" to work
      val operations =
        s"""/** Applies an action to the underlying $name. */
           |def action[U](f: Underlying$name[T] => U): Task[U] = ZIO.attempt(get(f))
           |
           |/** Applies a transformation to the underlying $name. */
           |def transformation[U](f: Underlying$name[T] => Underlying$name[U]): $name[U] = $name(get(f))""".stripMargin

      val get =
        s"""/** Applies an action to the underlying $name. */
           |def get[U](f: Underlying$name$tparam => U): U = f(underlying$name)""".stripMargin

      val getWithAnalysis =
        s"""/**
           | * Wraps a function into a TryAnalysis.
           | */
           |def getWithAnalysis[U](f: Underlying$name$tparam => U): TryAnalysis[U] =
           |  TryAnalysis(get(f))""".stripMargin

      val gets = get + "\n\n" + getWithAnalysis

      val transformations =
        s"""/**
           | * Applies a transformation to the underlying $name.
           | */
           |def transformation(f: Underlying$name => UnderlyingDataFrame): DataFrame =
           |  Dataset(f(underlying$name))
           |
           |/**
           | * Applies a transformation to the underlying $name, it is used for
           | * transformations that can fail due to an AnalysisException.
           | */
           |def transformationWithAnalysis(f: Underlying$name => UnderlyingDataFrame): TryAnalysis[DataFrame] =
           |  TryAnalysis(transformation(f))
           |""".stripMargin

      fold {
        case RDDPlan => operations + "\n\n" + get
        case DatasetPlan =>
          s"""
             |$operations
             |
             |/**
             | * Applies a transformation to the underlying dataset, it is used for
             | * transformations that can fail due to an AnalysisException.
             | */
             |def transformationWithAnalysis[U](f: UnderlyingDataset[T] => UnderlyingDataset[U]): TryAnalysis[Dataset[U]] =
             |  TryAnalysis(transformation(f))
             |
             |$gets""".stripMargin
        case DataFrameNaFunctionsPlan   => transformations
        case DataFrameStatFunctionsPlan => gets + "\n\n" + transformations
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
              |import zio._
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
            """import org.apache.spark.sql.{
              |  Column,
              |  Dataset => UnderlyingDataset,
              |  DataFrameNaFunctions => UnderlyingDataFrameNaFunctions,
              |  DataFrameStatFunctions => UnderlyingDataFrameStatFunctions,
              |  RelationalGroupedDataset => UnderlyingRelationalGroupedDataset,
              |  Encoder,
              |  Row,
              |  TypedColumn,
              |  Sniffer
              |}
              |import org.apache.spark.sql.types.StructType
              |import org.apache.spark.storage.StorageLevel
              |
              |import zio._
              |import zio.spark.rdd._
              |
              |import scala.reflect.runtime.universe.TypeTag
              |""".stripMargin

          val datasetSpecificImports =
            scalaBinaryVersion match {
              case ScalaBinaryVersion.V2_13 =>
                s"""import org.apache.spark.sql.execution.ExplainMode
                   |import scala.jdk.CollectionConverters._
                   |""".stripMargin
              case _ =>
                s"""import org.apache.spark.sql.execution.command.ExplainCommand
                   |import scala.collection.JavaConverters._
                   |""".stripMargin
            }

          s"""$datasetCommonImports
             |$datasetSpecificImports""".stripMargin
        case DataFrameNaFunctionsPlan =>
          """import org.apache.spark.sql.{
            | DataFrame => UnderlyingDataFrame,
            | DataFrameNaFunctions => UnderlyingDataFrameNaFunctions
            |}
            |""".stripMargin
        case DataFrameStatFunctionsPlan =>
          """import org.apache.spark.sql.{
            |  Column,
            |  DataFrame => UnderlyingDataFrame,
            |  DataFrameStatFunctions => UnderlyingDataFrameStatFunctions
            |}
            |import org.apache.spark.util.sketch.{BloomFilter, CountMinSketch}
            |""".stripMargin
      }

    final def suppressWarnings: String =
      fold {
        case RDDPlan =>
          "@SuppressWarnings(Array(\"scalafix:DisableSyntax.defaultArgs\", \"scalafix:DisableSyntax.null\"))"
        case _ => ""
      }

    final def sourceCode(body: String, overlay: String, scalaBinaryVersion: ScalaBinaryVersion): String =
      s"""/**
         | * /!\\ Warning /!\\
         | *
         | * This file is generated using zio-spark-codegen, you should not edit
         | * this file directly.
         | */
         |
         |package $zioSparkPackage
         |
         |${imports(scalaBinaryVersion)}
         |
         |$suppressWarnings
         |$definition { self =>
         |$implicits
         |
         |$helpers
         |
         |  // Handmade functions specific to zio-spark
         |  
         |$overlay
         |
         |  // Generated functions coming from spark
         |  
         |$body
         |
         |}
         |""".stripMargin

    final def getGenerationPlan(
        itSource: File,
        classpath: GetSources.Classpath,
        version: ScalaBinaryVersion
    ): zio.Task[GenerationPlan] =
      for {
        sparkSources <- GetSources.getSource(module, path)(classpath)
        overlaySources         = sourceFromFile(itSource / s"${name}Overlay.scala")
        overlaySpecificSources = sourceFromFile(versioned(itSource, version) / s"${name}OverlaySpecific.scala")
      } yield GenerationPlan(self, sparkSources, overlaySources, overlaySpecificSources, version)

    @inline final def fold[C](planType: PlanType => C): C = planType(this)

    final def fold[C](rdd: => C, dataset: => C, dataFrameNa: => C, dataFrameStat: => C): C =
      this match {
        case RDDPlan                    => rdd
        case DatasetPlan                => dataset
        case DataFrameNaFunctionsPlan   => dataFrameNa
        case DataFrameStatFunctionsPlan => dataFrameStat
      }
  }

  case object RDDPlan extends PlanType("spark-core", "org/apache/spark/rdd/RDD.scala")

  case object DatasetPlan extends PlanType("spark-sql", "org/apache/spark/sql/Dataset.scala")

  case object DataFrameNaFunctionsPlan extends PlanType("spark-sql", "org/apache/spark/sql/DataFrameNaFunctions.scala")

  case object DataFrameStatFunctionsPlan extends PlanType("spark-sql", "org/apache/spark/sql/DataFrameStatFunctions.scala")

  def sourceFromFile(file: File): Option[Source] = Try(IO.read(file)).toOption.flatMap(_.parse[Source].toOption)

  def checkMods(mods: List[Mod]): Boolean =
    !mods.exists {
      case mod"@DeveloperApi" => true
      case mod"private[$_]"   => true
      case mod"protected[$_]" => true
      case _                  => false
    }

  def collectFunctionsFromTemplate(template: TemplateWithComments): immutable.Seq[Defn.Def] =
    template.stats.collect { case d: Defn.Def if checkMods(d.mods) => d }

  def getTemplateFromSourceOverlay(source: Source): TemplateWithComments =
    new TemplateWithComments(source.children.collectFirst { case c: Defn.Class => c.templ }.get, true)

  def getTemplateFromSource(source: Source): TemplateWithComments =
    new TemplateWithComments(
      source.children
        .flatMap(_.children)
        .collectFirst { case c: Defn.Class => c.templ }
        .get,
      false
    )

}
