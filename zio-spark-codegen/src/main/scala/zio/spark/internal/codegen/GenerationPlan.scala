package zio.spark.internal.codegen

import zio.spark.internal.codegen.structure.Method

import scala.collection.immutable
import scala.meta.*
import scala.meta.contrib.AssociatedComments

case class GenerationPlan(module: String, path: String, source: meta.Source) {
  import GenerationPlan.*

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
        case mod"@DeveloperApi" => true
        case mod"private[$_]"   => true
        case mod"protected[$_]" => true
        case _                  => false
      }

    val allMethods: immutable.Seq[Defn.Def] =
      template.stats.collect {
        case d: Defn.Def if checkMods(d.mods) => d
        // case d: Decl.Def if checkMods(d.mods) => ??? // only compute is declared
      }

    val comments: AssociatedComments = contrib.AssociatedComments(template)

    allMethods.map(m => Method.fromScalaMeta(m, comments, path.replace('/', '.').replace(".scala", "")))
  }

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

    s"""// scalafix:off
       |$implicits
       |// scalafix:on
       |""".stripMargin
  }

  def imports: String = {
    val rddImports =
      """import org.apache.hadoop.io.compress.CompressionCodec
        |import org.apache.spark.{Dependency, Partition, Partitioner, TaskContext}
        |import org.apache.spark.partial.{BoundedDouble, PartialResult}
        |import org.apache.spark.rdd.{PartitionCoalescer, RDD => UnderlyingRDD, RDDBarrier}
        |import org.apache.spark.resource.ResourceProfile
        |import org.apache.spark.storage.StorageLevel
        |
        |import zio.Task
        |import zio.spark.impure.Impure
        |import zio.spark.impure.Impure.ImpureBox
        |import zio.spark.rdd.RDD
        |
        |import scala.collection.Map
        |import scala.io.Codec
        |import scala.reflect._""".stripMargin

    val datasetImports =
      """import org.apache.spark.sql.{Column, Dataset => UnderlyingDataset, Encoder, Row, TypedColumn}
        |import org.apache.spark.sql.types.StructType
        |import org.apache.spark.storage.StorageLevel
        |
        |import zio.Task
        |import zio.spark.impure.Impure
        |import zio.spark.impure.Impure.ImpureBox
        |import zio.spark.sql.{DataFrame, Dataset, TryAnalysis}
        |
        |import scala.jdk.CollectionConverters._
        |import scala.reflect.runtime.universe.TypeTag""".stripMargin

    planType.fold(rddImports, datasetImports)
  }

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
