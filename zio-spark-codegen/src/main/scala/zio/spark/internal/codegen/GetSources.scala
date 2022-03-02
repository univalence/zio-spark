package zio.spark.internal.codegen

import sbt.internal.util.Attributed

import zio.{Task, ZManaged}

import scala.collection.immutable
import scala.meta.*
import scala.meta.tokens.Token

import java.io.File

object GetSources {

  def getSource(module: String, file: String)(classpath: sbt.Def.Classpath): zio.Task[meta.Source] =
    Task {
      import scala.io.{BufferedSource, Source}
      import java.io.InputStream
      import java.util.jar.JarFile
      import java.util.zip.ZipEntry

      // val sparkCoreJar: String   = System.getProperty("java.class.path").split(':').find(_.contains(module)).get
      // val sparkSourceJar: String = sparkCoreJar.replace(".jar", "-sources.jar")
      /* val sparkSourceJar =
       * "/Users/dylandoamaral/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/spark/spark-core_2.12/3.2.1/spark-core_2.12-3.2.1-sources.jar" */

      @deprecated
      val hardSourceJar: String =
        System.getProperty(
          "user.home"
        ) + s"/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/spark/${module}_2.12/3.2.1/${module}_2.12-3.2.1-sources.jar"

      // TODO fix when running sbt test from command line
      val jar = classpath.find(_.data.getAbsolutePath.contains("/" + module + "_"))

      val sourceJar = jar.map(_.data.getAbsolutePath.replace(".jar", "-sources.jar")).get // .getOrElse(hardSourceJar)

      ZManaged
        .acquireReleaseAttemptWith(new JarFile(sourceJar))(_.close())
        .use(jarFile =>
          Task {
            val entry: ZipEntry         = jarFile.getEntry(file)
            val stream: InputStream     = jarFile.getInputStream(entry)
            val content: BufferedSource = Source.fromInputStream(stream)

            content.getLines().mkString("\n").parse[meta.Source].get
          }
        )
    }.flatten.onError { _ =>
      def red(text: String) = "\u001B[31m" + text + "\u001B[0m"

      zio.UIO(println(s"[${red("error")}] can't find $file in $module from $classpath"))
    }

  type Classpath = Seq[Attributed[File]]

  val defaultClasspath: Classpath = System.getProperty("java.class.path").split(':').map(x => Attributed.blank(new File(x)))

  def main(args: Array[String]): Unit = {

    val rddFileSource = zio.Runtime.default.unsafeRun(getSource("spark-core", "org/apache/spark/rdd/RDD.scala")(defaultClasspath))

    // source -> packages -> statements (imports | class | object)
    val rddTemplate: Template =
      rddFileSource.children
        .flatMap(_.children)
        .collectFirst {
          case c: Defn.Class if c.name.toString == "RDD" => c.templ
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
      rddTemplate.stats.collect {
        case d: Defn.Def if checkMods(d.mods) => d
        case d: Decl.Def if checkMods(d.mods) => ??? // only compute is declared
      }

    val persistMethod: Defn.Def = allMethods.find(_.name.structure == q"persist".structure).get
    val allComments             = contrib.AssociatedComments(rddTemplate)

    // Dylan va être content ...
    val commentForMethod: Set[Token.Comment] = allComments.leading(persistMethod)

    /**
     * Set this RDD's storage level to persist its values across
     * operations after the first time it is computed. This can only be
     * used to assign a new storage level if the RDD does not have a
     * storage level set yet. Local checkpointing is an exception.
     */

    val allDefinitions = allMethods.map(dfn => dfn.toString().replace(s" = ${dfn.body.toString()}", ""))

    val allReturnTypes =
      allDefinitions.map(_.parse[Stat].get).collect { case q"..$mods def $ename[..$tparams](...$paramss): $tpeopt = $expr" =>
        expr.pos
      }
    val a = 1
  }

}
