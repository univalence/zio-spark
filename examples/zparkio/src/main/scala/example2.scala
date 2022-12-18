package example.example2

/**
 * This code compare example2 to zio-spark
 * https://github.com/leobenkel/ZparkIO/tree/main/examples/Example2_small
 * the sub-project is in this single file
 */

import example.example2.Transformations.UserTransformations
import example.example2.services.{Database, FileIO, SparkBuilder}
import example.example2.services.Database.{Credentials, Database}
import example.example2.services.FileIO.FileIO
import org.apache.spark.SparkException
import org.apache.spark.sql.Encoder

import zio.{RIO, Task, ZIO, ZLayer}
import zio.spark.sql.{Dataset, SIO, SparkSession}
import zio.spark.sql.implicits._

package items {
  final case class Post(postId: Int, authorId: Int, title: String, content: String)
  final case class User(userId: Int, name: String, age: Int, active: Boolean)
}

package services {
  import scala.io.Source

  object Database {

    type Database = Service
    final case class Credentials(user: String, psw: String, host: String)

    /* https://github.com/leobenkel/ZparkIO/blob/main/examples/Example2_small/src/main/scala/com/leobenkel/example2/Services/Database.scala#L19-L30 */
    // Notes :
    // * You don't need in zio-spark to wrap the calls to get the SparkSession
    // * You don't need to import encoders
    // * You don't need to import org.apache.spark.sql.SparkSession#implicits
    // Original source :

    trait Service {
      def query[A: Encoder](query: String): SIO[Dataset[A]]
    }

    final case class LiveService(credentials: Credentials) extends Database.Service {
      override def query[A: Encoder](query: String): SIO[Dataset[A]] =
        // implicits are imported at the beginning of the file
        Seq.empty[A].toDS
    }

    // we are using zio-cli for args
    val Live: ZLayer[Arguments /*CommandLineArguments[Arguments]*/, Throwable, Database] =
      ZLayer.fromZIO(ZIO.serviceWith[Arguments](args => LiveService(args.credentials)))

    def apply[A: Encoder](query: String): RIO[Database with SparkSession, Dataset[A]] =
      ZIO.serviceWithZIO[Database](_.query(query))

  }

  // Nothing to change on this file : it's not related to Spark
  /* https://github.com/leobenkel/ZparkIO/blob/main/examples/Example2_small/src/main/scala/com/leobenkel/example2/Services/FileIO.scala */
  object FileIO {
    type FileIO = Service

    trait Service {
      protected def readFileContent(path: String): Task[Seq[String]]

      final def getFileContent(path: String): ZIO[Any, Throwable, Seq[String]] = readFileContent(path)
    }

    trait LiveService extends FileIO.Service {
      @SuppressWarnings(Array("scalafix:Disable.close", "scalafix:Disable.fromFile"))
      override protected def readFileContent(path: String): Task[Seq[String]] =
        ZIO.attempt {
          val file    = Source.fromFile(path)
          val content = file.getLines().toArray

          file.close()
          content
        }
    }

    val Live: ZLayer[Any, Throwable, FileIO] = ZLayer.succeed(new LiveService {})

    def apply(path: String): ZIO[FileIO, Throwable, Seq[String]] = ZIO.serviceWithZIO[FileIO](_.getFileContent(path))

  }

  // zio-spark is not having a Factory for the spark session
  // you can directly build the session using an existing factory on org.apache.spark.sql or
  // or using zio.spark.sql.SparkSession.Builder which is very close to the org.apache.spark.sql API
  object SparkBuilder /*extends SparkModule.Factory[Arguments]*/ {
    lazy val appName: String = "Zparkio_test"

    def updateConfig(sparkBuilder: SparkSession.Builder, arguments: Arguments): SparkSession.Builder =
      sparkBuilder.config("spark.foo.bar", arguments.sparkConfig)
  }

}

package Source {

  import example.example2.items.{Post, User}
  import example.example2.services.Database
  import example.example2.services.Database.Database

  object DatabaseSource {
    def getUsers: RIO[SparkSession with Database, Dataset[User]] =
      // No need for implicits imports from a local variable
      // just use : import zio.spark.sql.implicits._  at the beginning of the file
      /* for { spark <- SparkModule() users <- { import spark.implicits._ Database[User]("SELECT * FROM users") } }
       * yield users */
      Database[User]("SELECT * FROM users")

    def getPosts: RIO[SparkSession with Database, Dataset[Post]] =
      // No need for implicits imports from a local variable to get the encoders for Post
      // just use : import zio.spark.sql.implicits._  at the beginning of the file
      /* for { spark <- SparkModule() users <- { import spark.implicits._ Database[Post]("SELECT * FROM posts") } }
       * yield users */
      Database[Post]("SELECT * FROM users")
  }

}

package Transformations {

  import example.example2.Source.DatabaseSource
  import example.example2.items.User
  import example.example2.services.Database.Database

  object UserTransformations {
    def getAuthors: RIO[Database with SparkSession, Dataset[User]] =
      for {
        /* One advantage of ZIO here is the forking of the source fetch.
         * All source can be fetch in parallel.
         * Without ZIO, spark would just seat there while waiting for the
         *  first source to be retrieve before sending the
         * next query to the database. */
        usersF <- DatabaseSource.getUsers.fork
        postsF <- DatabaseSource.getPosts.fork
        users  <- usersF.join
        posts  <- postsF.join
        authorIds <- {
          // Dataset#collect in zio-spark is an effect. We don't need to wrap it in an attempt
          val getAuthors = posts.map(_.authorId).distinct.collect
          getAuthors.flatMap { ids =>
            // we don't have broadcast mapped in zio-spark as a method
            // we can use the retrocompat SparkSession.attempt to use existing org.apache.spark methods
            SparkSession.attempt(_.sparkContext.broadcast(ids))
          }
        }
      } yield users.filter(u => authorIds.value.contains(u.userId))
  }

}

//We don't have an APP, we can use is needed zio.ZIOAppDefault
// Roadmap for zio-spark : maybe we need to make a ZioSparkAppDefault
trait Application /*extends ZparkioApp[Arguments, APP_ENV, Unit]*/ {
  trait APP_ENV

  protected def env: ZLayer[Arguments, Throwable, FileIO with Database] = FileIO.Live ++ Database.Live

  protected def sparkFactory: SparkBuilder.type = SparkBuilder
  // protected def loggerFactory  = zio.logging
  // TODO : Show examples with a logger (reusing loggers in Spark with an effect system wrapper)

  protected def makeCli(args: List[String]): Arguments = ??? // Arguments(args)

  // zio-spark can use zio-cli (see example)

  def runApp: ZIO[SparkSession with Database, Throwable, Unit] =
    for {
      _       <- ZIO.logInfo(s"--Start--")
      authors <- UserTransformations.getAuthors
      // in zio-spark, Dataset#count *is an effect*, so you don't need to wrap it up in an attempt
      count <- authors.count
      _     <- ZIO.logInfo(s"There are $count authors")
    } yield ()

  /*
  def processErrors(f: Throwable): Option[Int] =
    // println(f)
    // f.printStackTrace(System.out)

    f match {
      case _: SparkException       => Some(10)
      case _: InterruptedException => Some(0)
      case _                       => Some(1)
    }
   */
}

object Application {
  type APP_ENV = FileIO with Database
}

final case class Arguments(
    databaseUsername: String,
    sparkConfig:      String
    /**
     * **
     */
    ,
    credentials: Credentials
)
