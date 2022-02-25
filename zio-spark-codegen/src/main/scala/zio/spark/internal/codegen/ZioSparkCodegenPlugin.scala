package zio.spark.internal.codegen

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import sbt.*
import sbt.Keys.*

import zio.spark.internal.codegen.GenerationPlan.PlanType
import zio.spark.internal.codegen.GetSources.getSource
import zio.spark.internal.codegen.ImportUtils.*
import zio.spark.internal.codegen.RDDAnalysis.*
import zio.spark.internal.codegen.structure.Method

import scala.collection.immutable
import scala.meta.*

case class GenerationPlan(module: String, path: String) {

  val planType: PlanType =
    path match {
      case "org/apache/spark/rdd/RDD.scala"     => GenerationPlan.RDDPlan
      case "org/apache/spark/sql/Dataset.scala" => GenerationPlan.DatasetPlan
    }

  def name: String = path.replace(".scala", "").split('/').last

  def pkg: String = path.replace(".scala", "").replace('/', '.')

  def sparkZioPath: String = pkg.replace("org.apache.spark", "zio.spark")

  lazy val methods: Seq[Method] = {
    val fileSource = zio.Runtime.default.unsafeRun(getSource(module, path))

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

    allMethods.map(m => Method.fromScalaMeta(m, path.replace('/', '.').replace(".scala", "")))
  }

  def baseImplicits: String = {
    val encoder: String = planType.fold("", "(implicit enc: Encoder[Seq[U]])")

    s"""private implicit def arrayToSeq1[U](x: $name[Array[U]])$encoder: $name[Seq[U]] = x.map(_.toSeq)
       |private implicit def arrayToSeq2[U](x: Underlying$name[Array[U]])$encoder: Underlying$name[Seq[U]] = x.map(_.toSeq)
       |private implicit def lift[U](x:Underlying$name[U]):$name[U] = $name(x)
       |private implicit def escape[U](x:$name[U]):Underlying$name[U] = x.underlying$name.succeedNow(v => v)
       |
       |private implicit def iteratorConversion[T](iterator: java.util.Iterator[T]):Iterator[T] = scala.collection.JavaConverters.asScalaIteratorConverter(iterator).asScala
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
        |import org.apache.spark.sql.{Dataset => UnderlyingDataset, Column, Encoder, Row, TypedColumn}
        |import org.apache.spark.storage.StorageLevel
        |
        |import zio.Task
        |import zio.spark.impure.Impure
        |import zio.spark.impure.Impure.ImpureBox
        |import zio.spark.sql.Dataset
        |""".stripMargin

    planType.fold(rddImports, datasetImports)
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

  lazy val rddPlan: GenerationPlan     = GenerationPlan("spark-core", "org/apache/spark/rdd/RDD.scala")
  lazy val datasetPlan: GenerationPlan = GenerationPlan("spark-sql", "org/apache/spark/sql/Dataset.scala")
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

        val generationPlans =
          List(
            GenerationPlan.rddPlan,
            GenerationPlan.datasetPlan
          )

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

          val methodsWithMethodTypes = methods.groupBy(getMethodType(_, plan.path))

          val body: String =
            methodsWithMethodTypes.toList
              .sortBy(_._1)
              .map { case (methodType, methods) =>
                val allMethods = methods.sortBy(_.fullName).map(_.toCode(methodType)).distinct.mkString("\n")
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
               |abstract class Base${plan.name}[T](underlying${plan.name}: ImpureBox[Underlying${plan.name}[T]]) extends Impure[Underlying${plan.name}[T]](underlying${plan.name}) {
               |  import underlying${plan.name}._
               |
               |${prefixAllLines(plan.baseImplicits, "  ")}
               |  
               |  /** Applies an action to the underlying ${plan.name}. */
               |  def action[U](f: Underlying${plan.name}[T] => U): Task[U] = attemptBlocking(f)
               |
               |  /** Applies a transformation to the underlying ${plan.name}. */
               |  def transformation[U](f: Underlying${plan.name}[T] => Underlying${plan.name}[U]): ${plan.name}[U] = succeedNow(f.andThen(x => ${plan.name}(x)))
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
