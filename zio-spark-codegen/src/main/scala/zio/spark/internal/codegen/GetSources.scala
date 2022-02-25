package zio.spark.internal.codegen

import scala.collection.immutable
import scala.io.{BufferedSource, Source}
import scala.meta.*
import scala.meta.tokens.Token

import java.io.InputStream
import java.util.jar.JarFile
import java.util.zip.ZipEntry

object GetSources {

  def main(args: Array[String]): Unit = {
    val sparkCoreJar: String = System.getProperty("java.class.path").split(':').find(_.contains("spark-core")).get

    val sparkSourceJar: String = sparkCoreJar.replace(".jar", "-sources.jar")

    val jarFile: JarFile        = new JarFile(sparkSourceJar)
    val entry: ZipEntry         = jarFile.getEntry("org/apache/spark/rdd/RDD.scala")
    val stream: InputStream     = jarFile.getInputStream(entry)
    val content: BufferedSource = Source.fromInputStream(stream)

    val rddFileSource: meta.Source = content.getLines().mkString("\n").parse[meta.Source].get

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

    val lines = content.getLines().size
  }

}
