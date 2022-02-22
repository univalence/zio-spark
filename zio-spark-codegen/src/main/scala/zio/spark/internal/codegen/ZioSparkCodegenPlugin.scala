package zio.spark.internal.codegen

import sbt.*
import sbt.Keys.*
import zio.spark.internal.codegen.RDDAnalysis.*

import scala.reflect.runtime.universe

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
        val apacheSparkMethods: Seq[universe.MethodSymbol] = readMethodsApacheSparkRDD
        val methodsToImplement: Seq[universe.MethodSymbol] = apacheSparkMethods.filter(s => !zioSparkMethodNames(s.fullName))


        def listMethods(methods: Seq[universe.MethodSymbol]): String = {
          methods.map(_.fullName).filter(!_.contains("$")).filter(!_.contains("java.lang.Object")).filter(!_.contains("scala.Any")).distinct.map("[[" + _ + "]]").sliding(3, 3).map(_.mkString(", ")).map(" * " + _).mkString("\n")
        }

        val body:String = readMethodsApacheSparkRDD.groupBy(getMethodType).map({
          case (MethodType.Ignored, methods) =>
            "/** Ignored methods : \n" + listMethods(methods) + "\n */"
          case (MethodType.ToImplement, methods) =>
            "/** Methods to implement : \n" + listMethods(methods) + "\n */"
          case (MethodType.SuccessNow, methods) => methods.map {
            method =>
              val methodCall:String = if(method.paramLists == List(Nil)) s"${method.name}()" else method.name.toString
              s"def ${method.name}: ${method.returnType} = succeedNow(_.$methodCall)"
          }.mkString("\n\n")

          case _ => ""
        }).mkString("\n//---------\n")



        IO.write(
          file,
          s"""package zio.spark.internal.codegen
            |
            |import org.apache.spark.rdd.RDD
            |
            |import zio.spark.impure.Impure
            |import zio.spark.impure.Impure.ImpureBox
            |
            |abstract class BaseRDD[T](underlyingDataset: ImpureBox[RDD[T]]) extends Impure[RDD[T]](underlyingDataset) {
            |  import underlyingDataset._
            |
            |${body.split("\n").map("  " + _).mkString("\n")}
            |}
            |""".stripMargin
        )
        Seq(file)
      }.taskValue
    )


}
