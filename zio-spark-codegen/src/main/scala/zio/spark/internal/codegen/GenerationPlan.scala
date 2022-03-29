package zio.spark.internal.codegen

import sbt.*
import sbt.Keys.Classpath

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
    functionsFromFile(source, filterOverlay = false)
      .filterNot(_.fullName.contains("$"))
      .filterNot(_.fullName.contains("java.lang.Object"))
      .filterNot(_.fullName.contains("scala.Any"))
      .filterNot(_.fullName.contains("<init>"))
      .filterNot(_.anyParameters.map(_.signature).exists(_.contains("ju.")))   // Java specific implementation
      .filterNot(_.anyParameters.map(_.signature).exists(_.contains("jl.")))   // Java specific implementation
      .filterNot(_.anyParameters.map(_.signature).exists(_.contains("java")))  // Java specific implementation
      .filterNot(_.anyParameters.map(_.signature).exists(_.contains("Array"))) // Java specific implementation

  lazy val overlayMethods: Seq[Method] =
    overlay.map(functionsFromFile(_, filterOverlay = true)).getOrElse(Seq.empty) ++ overlaySpecific
      .map(functionsFromFile(_, filterOverlay = true))
      .getOrElse(Seq.empty)

  lazy val methods: Seq[Method] = sourceMethods ++ overlayMethods
}

object GenerationPlan {
  case class Module(name: String, path: String)
  val coreModule: Module = Module("spark-core", "org/apache/spark/rdd")
  val sqlModule: Module  = Module("spark-sql", "org/apache/spark/sql")

  sealed abstract class PlanType(module: Module, val className: String) {
    self =>
    final def path: String = s"${module.path}/$className.scala"

    final def name: String = path.replace(".scala", "").split('/').last

    final def pkg: String = path.replace(".scala", "").replace('/', '.')

    final def zioSparkPath: String = path.replace("org/apache/spark", "zio/spark")

    final def zioSparkPackage: String = zioSparkPath.split("/").dropRight(1).mkString(".")

    final def typeParameters: List[String] =
      fold {
        case RDDPlan | DatasetPlan      => List("T")
        case KeyValueGroupedDatasetPlan => List("K", "V")
        case _                          => Nil
      }

    final def hasTypeParameter: Boolean = typeParameters.nonEmpty

    final def tparam: String = if (hasTypeParameter) s"[${typeParameters.mkString(", ")}]" else ""

    final def definition: String = {
      val className      = s"$name$tparam"
      val underlyingName = s"Underlying$name$tparam"

      s"final case class $className(underlying: $underlyingName)"
    }

    final def implicits(scalaBinaryVersion: ScalaBinaryVersion): String = {
      val lift = s"private implicit def lift[U](x:Underlying$name[U]):$name[U] = $name(x)"

      val allImplicits =
        fold {
          case RDDPlan =>
            s"""$lift
               |
               |private implicit def arrayToSeq2[U](x: Underlying$name[Array[U]]): Underlying$name[Seq[U]] = x.map(_.toIndexedSeq)
               |@inline private def noOrdering[U]: Ordering[U] = null""".stripMargin
          case DatasetPlan =>
            s"""$lift
               |
               |private implicit def iteratorConversion[U](iterator: java.util.Iterator[U]):Iterator[U] =
               |  iterator.asScala
               |private implicit def liftDataFrameNaFunctions[U](x: UnderlyingDataFrameNaFunctions): DataFrameNaFunctions =
               |  DataFrameNaFunctions(x)
               |private implicit def liftDataFrameStatFunctions[U](x: UnderlyingDataFrameStatFunctions): DataFrameStatFunctions =
               |  DataFrameStatFunctions(x)""".stripMargin
          case RelationalGroupedDatasetPlan =>
            scalaBinaryVersion match {
              case ScalaBinaryVersion.V2_11 => ""
              case _ =>
                s"""implicit private def liftKeyValueGroupedDataset[K, V](
                   |  x: UnderlyingKeyValueGroupedDataset[K, V]
                   |): KeyValueGroupedDataset[K, V] = KeyValueGroupedDataset(x)""".stripMargin
            }

          case _ => ""
        }

      if (allImplicits.nonEmpty) {
        s"""  // scalafix:off
           |$allImplicits
           |  // scalafix:on
           |""".stripMargin
      } else allImplicits
    }

    import GenerationPlan.Helper.*

    final def helpers: Helper =
      fold {
        case RDDPlan                      => action && transformation && get
        case DatasetPlan                  => action && transformations && gets
        case DataFrameNaFunctionsPlan     => unpacks
        case DataFrameStatFunctionsPlan   => unpacks && transformations && gets
        case RelationalGroupedDatasetPlan => unpacks && transformations && gets
        case KeyValueGroupedDatasetPlan   => unpacks && transformation
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
              case ScalaBinaryVersion.V2_11 =>
                s"""import org.apache.spark.rdd.RDDBarrier
                   |""".stripMargin
              case _ =>
                s"""import org.apache.spark.rdd.RDDBarrier
                   |import org.apache.spark.resource.ResourceProfile
                   |""".stripMargin
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
              case ScalaBinaryVersion.V2_12 =>
                s"""import org.apache.spark.sql.execution.ExplainMode
                   |import scala.collection.JavaConverters._
                   |""".stripMargin
              case ScalaBinaryVersion.V2_11 =>
                s"""import org.apache.spark.sql.execution.command.ExplainCommand
                   |import scala.collection.JavaConverters._
                   |""".stripMargin
            }

          s"""$datasetCommonImports
             |$datasetSpecificImports""".stripMargin
        case DataFrameNaFunctionsPlan =>
          """import org.apache.spark.sql.{
            | Dataset => UnderlyingDataset, 
            | DataFrameNaFunctions => UnderlyingDataFrameNaFunctions
            |}
            |""".stripMargin
        case DataFrameStatFunctionsPlan =>
          """import org.apache.spark.sql.{
            |  Column,
            |  Dataset => UnderlyingDataset, 
            |  DataFrameStatFunctions => UnderlyingDataFrameStatFunctions
            |}
            |import org.apache.spark.util.sketch.{BloomFilter, CountMinSketch}
            |""".stripMargin
        case RelationalGroupedDatasetPlan =>
          scalaBinaryVersion match {
            case ScalaBinaryVersion.V2_11 =>
              """import org.apache.spark.sql.{
                |  Column,
                |  Dataset => UnderlyingDataset,
                |  RelationalGroupedDataset => UnderlyingRelationalGroupedDataset
                |}
                |""".stripMargin
            case _ =>
              """import org.apache.spark.sql.{
                |  Column,
                |  Encoder,
                |  Dataset => UnderlyingDataset,
                |  RelationalGroupedDataset => UnderlyingRelationalGroupedDataset,
                |  KeyValueGroupedDataset => UnderlyingKeyValueGroupedDataset,
                |}
                |""".stripMargin
          }
        case KeyValueGroupedDatasetPlan =>
          """import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
            |import org.apache.spark.sql.{
            |  Encoder, 
            |  TypedColumn, 
            |  Dataset => UnderlyingDataset, 
            |  KeyValueGroupedDataset => UnderlyingKeyValueGroupedDataset
            |}
            |""".stripMargin
      }

    final def suppressWarnings: String =
      fold {
        case RDDPlan =>
          "@SuppressWarnings(Array(\"scalafix:DisableSyntax.defaultArgs\", \"scalafix:DisableSyntax.null\"))"
        case _ => ""
      }

    /** Generates the Zio-Spark source code. */
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
         |${implicits(scalaBinaryVersion)}
         |
         |${helpers(name, typeParameters)}
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
        classpath: Classpath,
        version: ScalaBinaryVersion
    ): zio.Task[GenerationPlan] =
      for {
        sparkSources <- GetSources.getSource(module.name, path)(classpath)
        overlaySources         = sourceFromFile(itSource / s"${name}Overlay.scala")
        overlaySpecificSources = sourceFromFile(versioned(itSource, version) / s"${name}OverlaySpecific.scala")
      } yield GenerationPlan(self, sparkSources, overlaySources, overlaySpecificSources, version)

    @inline final def fold[C](planType: PlanType => C): C = planType(this)
  }

  case object RDDPlan                      extends PlanType(coreModule, "RDD")
  case object DatasetPlan                  extends PlanType(sqlModule, "Dataset")
  case object DataFrameNaFunctionsPlan     extends PlanType(sqlModule, "DataFrameNaFunctions")
  case object DataFrameStatFunctionsPlan   extends PlanType(sqlModule, "DataFrameStatFunctions")
  case object RelationalGroupedDatasetPlan extends PlanType(sqlModule, "RelationalGroupedDataset")
  case object KeyValueGroupedDatasetPlan   extends PlanType(sqlModule, "KeyValueGroupedDataset")

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

  trait Helper {
    self =>
    def apply(name: String, typeParameters: List[String]): String

    def &&(other: Helper): Helper = (name: String, typeParameters: List[String]) => self(name, typeParameters) + "\n\n" + other(name, typeParameters)
  }

  object Helper {
    val strParams: List[String] => String =
      (typeParameters: List[String]) => if (typeParameters.nonEmpty) s"[${typeParameters.mkString(", ")}]" else ""

    val action: Helper = { (name, typeParameters) =>
      // NOTE : action need to stay an attempt, and not an attemptBlocked for the moment.
      // 1. The ZIO Scheduler will catch up and treat it as if it's an attemptBlocked
      // 2. It's necessary for "makeItCancellable" to work
      val tParam = if (typeParameters.nonEmpty) s"[${typeParameters.mkString(", ")}]" else ""
      s"""/** Applies an action to the underlying $name. */
         |def action[U](f: Underlying$name$tParam => U)(implicit trace: ZTraceElement): Task[U] = ZIO.attempt(get(f))""".stripMargin
    }

    val transformation: Helper = { (name, typeParameters) =>
      val tParam = strParams(typeParameters)
      val uParam = strParams(typeParameters.map(_ + "New"))
      s"""/** Applies a transformation to the underlying $name. */
         |def transformation$uParam(f: Underlying$name$tParam => Underlying$name$uParam): $name$uParam =
         |  $name(f(underlying))""".stripMargin
    }

    val transformationWithAnalysis: Helper = { (name, typeParameters) =>
      val tParam = strParams(typeParameters)
      val uParam = strParams(typeParameters.map(_ + "New"))
      s"""/** Applies a transformation to the underlying $name, it is used for
         | * transformations that can fail due to an AnalysisException. */
         |def transformationWithAnalysis$uParam(f: Underlying$name$tParam => Underlying$name$uParam): TryAnalysis[$name$uParam] =
         |  TryAnalysis(transformation(f))""".stripMargin
    }

    val transformations: Helper = transformation && transformationWithAnalysis

    val get: Helper = { (name, typeParameters) =>
      val tParam = strParams(typeParameters)
      s"""/** Applies an action to the underlying $name. */
         |def get[U](f: Underlying$name$tParam => U): U = f(underlying)""".stripMargin
    }

    val getWithAnalysis: Helper = { (name, typeParameters) =>
      val tParam = strParams(typeParameters)
      s"""/** Applies an action to the underlying $name, it is used for
         | * transformations that can fail due to an AnalysisException.
         | */
         |def getWithAnalysis[U](f: Underlying$name$tParam => U): TryAnalysis[U] =
         |  TryAnalysis(f(underlying))""".stripMargin
    }

    val gets: Helper = get && getWithAnalysis

    val unpack: Helper = { (name, typeParameters) =>
      val tParam = strParams(typeParameters)
      s"""/**
         | * Unpack the underlying $name into a DataFrame.
         | */
         |def unpack[U](f: Underlying$name$tParam => UnderlyingDataset[U]): Dataset[U] =
         |  Dataset(f(underlying))""".stripMargin
    }

    val unpackWithAnalysis: Helper = { (name, typeParameters) =>
      val tParam = strParams(typeParameters)
      s"""/**
         | * Unpack the underlying $name into a DataFrame, it is used for
         | * transformations that can fail due to an AnalysisException.
         | */
         |def unpackWithAnalysis[U](f: Underlying$name$tParam => UnderlyingDataset[U]): TryAnalysis[Dataset[U]] =
         |  TryAnalysis(unpack(f))""".stripMargin
    }

    val unpacks: Helper = unpack && unpackWithAnalysis
  }

}
