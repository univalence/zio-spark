package zio.spark.internal.codegen

import sbt.internal.util.Attributed

import zio.{Task, UIO, ZManaged}

import scala.collection.immutable
import scala.meta.*
import scala.meta.tokens.Token
import scala.util.matching.Regex

import java.io.File
import java.net.URLClassLoader
import java.nio.file.{Files, Path}
import java.util.jar.JarFile
object GetSources {

  val RootSparkPattern: Regex = "($(.*)/org/apache/spark)".r

  def findSourceJar(module: String, classpath: Classpath): zio.Task[JarFile] =
    Task {

      val path: String =
        classpath
          .map(_.data)
          .collectFirst {
            case f if f.getAbsolutePath.contains(module) =>
              f.getAbsolutePath.replaceFirst("\\.jar$", "-sources.jar")
          }
          .get

      new JarFile(new File(path))
    }.tap(jar => UIO(println(s"found $module in ${jar.getName}")))

  def red(text: String): String = "\u001B[31m" + text + "\u001B[0m"

  def getSource(module: String, file: String)(classpath: sbt.Def.Classpath): zio.Task[meta.Source] =
    Task {
      import scala.io.{BufferedSource, Source}
      import java.io.InputStream
      import java.util.zip.ZipEntry

      ZManaged
        .acquireReleaseWith(findSourceJar(module, classpath))(x => Task(x.close()).ignore)
        .use(jarFile =>
          Task {
            val entry: ZipEntry         = jarFile.getEntry(file)
            val stream: InputStream     = jarFile.getInputStream(entry)
            val content: BufferedSource = Source.fromInputStream(stream)

            content.getLines().mkString("\n").parse[meta.Source].get
          }
        )
    }.flatten.onError { _ =>
      zio.UIO(println(s"[${red("error")}] can't find $file in $module from $classpath"))
    }

  type Classpath = Seq[Attributed[File]]

  def classLoaderToClasspath(classLoader: ClassLoader): Classpath =
    classLoader match {
      case classLoader: URLClassLoader => classLoader.getURLs.map(_.getFile).map(x => Attributed.blank(new File(x)))
      case _                           => Seq.empty
    }
}
