package zio.spark.internal.codegen

import sbt.*
import sbt.Keys.*

object ZioSparkCodegenPlugin extends AutoPlugin {
  object autoImport {
    val sparkLibraryVersion = settingKey[String]("Specifies the version of Spark to depend on")
  }

  override lazy val projectSettings =
    Seq(
      Compile / sourceGenerators += Def.task {
        val file = (Compile / sourceManaged).value / "zio" / "spark" / "internal" / "codegen" / "Test.scala"
        IO.write(file,
          """
            |package zio.spark.internal.codegen
            |
            |object Test extends App { println("Hi") }
            |""".stripMargin)
        Seq(file)
      }.taskValue
    )
}
