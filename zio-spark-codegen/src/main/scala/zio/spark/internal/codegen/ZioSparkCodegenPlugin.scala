package zio.spark.internal.codegen

import sbt.*
import sbt.Keys.*

import zio.spark.internal.codegen.GenerationPlan.PlanType
import zio.spark.internal.codegen.ImportUtils.*
import zio.spark.internal.codegen.RDDAnalysis.*
import zio.spark.internal.codegen.structure.Method

import scala.collection.immutable
import scala.meta.*
import scala.meta.contrib.AssociatedComments

case class GenerationPlan(module: String, path: String, source: meta.Source) {

  val planType: PlanType =
    path match {
      case "org/apache/spark/rdd/RDD.scala"     => GenerationPlan.RDDPlan
      case "org/apache/spark/sql/Dataset.scala" => GenerationPlan.DatasetPlan
    }

  def name: String = path.replace(".scala", "").split('/').last

  def pkg: String = path.replace(".scala", "").replace('/', '.')

  def sparkZioPath: String = pkg.replace("org.apache.spark", "zio.spark")

  lazy val methods: Seq[Method] = {
    val fileSource = source

    val template: Template =
      fileSource.children
        .flatMap(_.children)
        .collectFirst {
          case c: Defn.Class if c.name.toString == "RDD"     => c.templ
          case c: Defn.Class if c.name.toString == "Dataset" => c.templ
        }
        .get

    def checkMods(mods: List[Mod]): Boolean =
      !mods.exists {
        case mod"@DeveloperApi"   => true
        case mod"private[$ref]"   => true
        case mod"protected[$ref]" => true
        case _                    => false
      }

    val allMethods: immutable.Seq[Defn.Def] =
      template.stats.collect {
        case d: Defn.Def if checkMods(d.mods) => d
        case d: Decl.Def if checkMods(d.mods) => ??? // only compute is declared
      }

    val comments: AssociatedComments = contrib.AssociatedComments(template)

    allMethods.map(m => Method.fromScalaMeta(m, comments, path.replace('/', '.').replace(".scala", "")))
  }

  def baseImplicits: String = {
    val encoder: String = planType.fold("", "(implicit enc: Encoder[Seq[U]])")

    s"""// scalafix:off
       |private implicit def arrayToSeq1[U](x: $name[Array[U]])$encoder: $name[Seq[U]] = x.map(_.toIndexedSeq)
       |private implicit def arrayToSeq2[U](x: Underlying$name[Array[U]])$encoder: Underlying$name[Seq[U]] = x.map(_.toIndexedSeq)
       |private implicit def lift[U](x:Underlying$name[U]):$name[U] = $name(x)
       |private implicit def escape[U](x:$name[U]):Underlying$name[U] = x.underlying$name.succeedNow(v => v)
       |private implicit def iteratorConversion[U](iterator: java.util.Iterator[U]):Iterator[U] = scala.collection.JavaConverters.asScalaIteratorConverter(iterator).asScala
       |
       |@inline private def noOrdering[U]: Ordering[U] = null
       |// scalafix:on
       |""".stripMargin
  }

  def imports: String = {
    val rddImports =
      """
        |import scala.reflect._
        |
        |import scala.io.Codec
        |
        |import org.apache.spark.partial.{PartialResult, BoundedDouble}
        |import org.apache.spark.rdd.{RDD => UnderlyingRDD, RDDBarrier, PartitionCoalescer}
        |import org.apache.spark.resource.ResourceProfile
        |import org.apache.spark.storage.StorageLevel
        |import org.apache.spark.{Partition, TaskContext, Dependency, Partitioner}
        |import org.apache.hadoop.io.compress.CompressionCodec
        |
        |import zio.Task
        |import zio.spark.impure.Impure
        |import zio.spark.impure.Impure.ImpureBox
        |import zio.spark.rdd.RDD
        |
        |import scala.collection.Map
        |""".stripMargin

    val datasetImports =
      """
        |import scala.reflect.runtime.universe.TypeTag
        |
        |import org.apache.spark.sql.{Dataset => UnderlyingDataset, Column, Encoder, Row, TypedColumn}
        |import org.apache.spark.storage.StorageLevel
        |import org.apache.spark.sql.types.StructType
        |
        |import zio.Task
        |import zio.spark.impure.Impure
        |import zio.spark.impure.Impure.ImpureBox
        |import zio.spark.sql.{DataFrame, Dataset, TryAnalysis}
        |""".stripMargin

    planType.fold(rddImports, datasetImports)
  }

  def helpers: String = {
    val defaultHelpers =
      s"""/** Applies an action to the underlying ${name}. */
         |def action[U](f: Underlying${name}[T] => U): Task[U] = attemptBlocking(f)
         |
         |/** Applies a transformation to the underlying ${name}. */
         |def transformation[U](f: Underlying${name}[T] => Underlying${name}[U]): ${name}[U] = succeedNow(f.andThen(x => ${name}(x)))""".stripMargin

    val datasetHelpers =
      """/**
        | * Applies a transformation to the underlying dataset, it is used for
        | * transformations that can fail due to an AnalysisException.
        | */
        |def transformationWithAnalysis[U](f: UnderlyingDataset[T] => UnderlyingDataset[U]): TryAnalysis[Dataset[U]] =
        |  TryAnalysis(transformation(f))
        |""".stripMargin

    defaultHelpers + "\n\n" + planType.fold("", datasetHelpers)
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

  private def get(module: String, file: String, classpath: GetSources.Classpath): zio.Task[GenerationPlan] =
    GetSources.getSource(module, file)(classpath).map(source => GenerationPlan(module, file, source))

  def rddPlan(classpath: GetSources.Classpath): zio.Task[GenerationPlan]     = get("spark-core", "org/apache/spark/rdd/RDD.scala", classpath)
  def datasetPlan(classpath: GetSources.Classpath): zio.Task[GenerationPlan] = get("spark-sql", "org/apache/spark/sql/Dataset.scala", classpath)

}

object ZioSparkCodegenPlugin extends AutoPlugin {
  object autoImport {
    val sparkLibraryVersion = settingKey[String]("Specifies the version of Spark to depend on")
  }

  private def readFinalClassRDD(scalaSource: File): Set[String] = {
    val file: File = scalaSource / "zio" / "spark" / "rdd" / "RDD.scala"

    import scala.meta.*
    val parsed: Source = IO.read(file).parse[Source].get

    val methods = scala.collection.mutable.TreeSet.empty[String]
    parsed.traverse {
      case m: Decl.Def if !m.mods.contains(Mod.Private) => methods.add(m.name.value)
      case _                                            => Unit
    }
    methods.toSet
  }

  def prefixAllLines(text: String, prefix: String): String = text.split("\n").map(prefix + _).mkString("\n")

  def commentMethods(methods: String, title: String): String =
    s"""/**
       | * $title
       | *
       |${prefixAllLines(methods, " * ")}
       | */""".stripMargin

  override lazy val projectSettings =
    Seq(
      Compile / sourceGenerators += Def.task {

        // TODO : use to get source jar
        val jars = (Compile / dependencyClasspathAsJars).value

        val generationPlans: immutable.Seq[GenerationPlan] =
          List(
            GenerationPlan.rddPlan(jars),
            GenerationPlan.datasetPlan(jars)
          ).map(zio.Runtime.default.unsafeRun)

        // TODO Check implementation
        // val zioSparkMethodNames: Set[String] = readFinalClassRDD((Compile / scalaSource).value)

        val generatedFiles =
          generationPlans.map { plan =>
            (Compile / scalaSource).value / "zio" / "spark" / "internal" / "codegen" / s"Base${plan.name}.scala"
          }

        generationPlans.zip(generatedFiles).foreach { case (plan, file) =>
          val methods =
            plan.methods
              .filterNot(_.fullName.contains("$"))
              .filterNot(_.fullName.contains("java.lang.Object"))
              .filterNot(_.fullName.contains("scala.Any"))
              .filterNot(_.fullName.contains("<init>"))

          val methodsWithMethodTypes = methods.groupBy(getMethodType)

          val body: String =
            methodsWithMethodTypes.toList
              .sortBy(_._1)
              .map { case (methodType, methods) =>
                val sep =
                  methodType match {
                    case MethodType.ToImplement => "\n"
                    case MethodType.Ignored     => "\n"
                    case _                      => "\n\n"
                  }

                val allMethods = methods.sortBy(_.fullName).map(_.toCode(methodType)).distinct.mkString(sep)
                methodType match {
                  case MethodType.ToImplement => commentMethods(allMethods, "Methods to implement")
                  case MethodType.Ignored     => commentMethods(allMethods, "Ignored method")
                  case _                      => allMethods
                }
              }
              .mkString("\n\n//===============\n\n")

          val imports =
            findImports(
              (methodsWithMethodTypes - (MethodType.ToImplement, MethodType.Ignored)).values.flatten.toSeq
            ).filterNot { case (pkg, _) => importedPackages.contains(pkg) }
              .map { case (pkg, objs) => generateImport(pkg, objs) }
              .map("import " + _)
              .filterNot(_.contains("java"))
              .toSeq
              .sorted
              .mkString("\n")

          IO.write(
            file,
            s"""package zio.spark.internal.codegen
               |
               |${plan.imports}
               |
               |
               |@SuppressWarnings(Array("scalafix:DisableSyntax.defaultArgs", "scalafix:DisableSyntax.null"))
               |abstract class Base${plan.name}[T](underlying${plan.name}: ImpureBox[Underlying${plan.name}[T]]) extends Impure[Underlying${plan.name}[T]](underlying${plan.name}) {
               |  import underlying${plan.name}._
               |
               |${prefixAllLines(plan.baseImplicits, "  ")}
               |  
               |${prefixAllLines(plan.helpers, "  ")}
               |
               |${prefixAllLines(body, "  ")}
               |}
               |""".stripMargin
          )
        }

        generatedFiles
      }.taskValue
    )
}
