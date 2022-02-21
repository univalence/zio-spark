package zio.spark.internal.codegen

import sbt.*
import sbt.Keys.*

import zio.spark.internal.codegen.RDDAnalysis._

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

  override lazy val projectSettings =
    Seq(
      Compile / sourceGenerators += Def.task {
        val file = (Compile / sourceManaged).value / "zio" / "spark" / "internal" / "codegen" / "BaseRDD.scala"

        val zioSparkMethodNames: Set[String] = readFinalClassRDD((Compile / scalaSource).value)
        val apacheSparkMethods               = readMethodsApacheSparkRDD
        val methodsToImplement               = apacheSparkMethods.filter(s => !zioSparkMethodNames(s.fullName))

        IO.write(
          file,
          """
            |package zio.spark.internal.codegen
            |
            |import org.apache.spark.sql.Dataset
            |
            |import zio.spark.impure.Impure
            |import zio.spark.impure.Impure.ImpureBox
            |
            |abstract class BaseRDD[T](underlyingDataset: ImpureBox[Dataset[T]])
            |    extends Impure[Dataset[T]](underlyingDataset) { 
            |  
            |}
            |""".stripMargin
        )
        Seq(file)
      }.taskValue
    )
}
