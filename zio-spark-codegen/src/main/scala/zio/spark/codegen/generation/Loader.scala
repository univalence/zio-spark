package zio.spark.codegen.generation

import sbt.File
import sbt.Keys.Classpath

import zio.{IO, UIO, ZIO}
import zio.spark.codegen.generation.Error.*

import scala.io.{BufferedSource, Source}
import scala.meta.*

import java.io.InputStream
import java.util.jar.JarFile
import java.util.zip.ZipEntry

object Loader {

  /**
   * Find the source jar file from the actual classpath.
   *
   * We need, in SBT, to download spark with sources.
   */
  private def findSourceJar(moduleName: String, classpath: Classpath): ZIO[Logger, CodegenError, JarFile] = {
    val maybePath: Option[String] =
      classpath
        .map(_.data)
        .collectFirst {
          case f if f.getAbsolutePath.contains(moduleName) =>
            f.getAbsolutePath.replaceFirst("\\.jar$", "-sources.jar")
        }

    maybePath match {
      case None => ZIO.fail(ModuleNotFoundError(moduleName))
      case Some(path) =>
        ZIO
          .attempt(new JarFile(new File(path)))
          .orDie
    }
  }

  /**
   * Read the source of particular file of a particular spark module
   * from sources and load the code in ScalaMeta.
   */
  def sourceFromClasspath(
      filePath: String,
      moduleName: String,
      classpath: Classpath
  ): ZIO[Logger, CodegenError, meta.Source] =
    ZIO.scoped {
      for {
        jar <- ZIO.acquireRelease(findSourceJar(moduleName, classpath))(x => ZIO.attempt(x.close()).ignore)
        _   <- Logger.info(s"Found $moduleName in ${jar.getName} for $filePath")
        source <-
          ZIO
            .attempt {
              val entry: ZipEntry         = jar.getEntry(filePath)
              val stream: InputStream     = jar.getInputStream(entry)
              val content: BufferedSource = Source.fromInputStream(stream)

              content.getLines().mkString("\n").parse[meta.Source].get
            }
            .mapError(SourceNotFoundError(filePath, moduleName, _))
      } yield source
    }

  /**
   * Retrieves the content of a Scala file as Scala meta source.
   * @param file
   *   The file to retrieve content from
   */
  def sourceFromFile(file: File): IO[CodegenError, meta.Source] =
    for {
      content <- ZIO.attempt(sbt.IO.read(file)).mapError(FileReadingError(file.getPath, _))
      source  <- ZIO.attempt(content.parse[meta.Source].get).mapError(ContentIsNotSourceError(file.getPath, _))
    } yield source

  /**
   * Retrieves the content of a Scala file as Scala meta source, returns
   * None if the file doesn't not exist.
   */
  def optionalSourceFromFile(file: File): IO[CodegenError, Option[meta.Source]] =
    sourceFromFile(file).foldZIO(
      failure = {
        case FileReadingError(_, _) => ZIO.succeed(None)
        case e                      => ZIO.fail(e)
      },
      success = source => ZIO.succeed(Some(source))
    )

}
