package zio.spark.internal.codegen

import scala.io.{BufferedSource, Source}

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

    val lines = content.getLines().size
  }

}
