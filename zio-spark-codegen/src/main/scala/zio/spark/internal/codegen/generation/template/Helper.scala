package zio.spark.internal.codegen.generation.template

trait Helper {
  self =>
  def apply(name: String, typeParameters: List[String]): String

  def &&(other: Helper): Helper =
    (name: String, typeParameters: List[String]) => self(name, typeParameters) + "\n\n" + other(name, typeParameters)
}

object Helper {
  def stringifyTypeParameters(typeParameters: List[String]): String =
    if (typeParameters.nonEmpty) s"[${typeParameters.mkString(", ")}]" else ""

  val nothing: Helper = { (_, _) => "" }

  val action: Helper = { (name, typeParameters) =>
    // NOTE : action need to stay an attempt, and not an attemptBlocked for the moment.
    // 1. The ZIO Scheduler will catch up and treat it as if it's an attemptBlocked
    // 2. It's necessary for "makeItCancellable" to work
    val tParam = stringifyTypeParameters(typeParameters)
    s"""/** Applies an action to the underlying $name. */
       |def action[U](f: Underlying$name$tParam => U)(implicit trace: ZTraceElement): Task[U] = 
       |  ZIO.attempt(get(f))""".stripMargin
  }

  val transformation: Helper = { (name, typeParameters) =>
    val tParam = stringifyTypeParameters(typeParameters)
    val uParam = stringifyTypeParameters(typeParameters.map(_ + "New"))
    s"""/** Applies a transformation to the underlying $name. */
       |def transformation$uParam(f: Underlying$name$tParam => Underlying$name$uParam): $name$uParam =
       |  $name(f(underlying))""".stripMargin
  }

  val transformationWithAnalysis: Helper = { (name, typeParameters) =>
    val tParam = stringifyTypeParameters(typeParameters)
    val uParam = stringifyTypeParameters(typeParameters.map(_ + "New"))
    s"""/** Applies a transformation to the underlying $name, it is used for
       | * transformations that can fail due to an AnalysisException. */
       |def transformationWithAnalysis$uParam(f: Underlying$name$tParam => Underlying$name$uParam): TryAnalysis[$name$uParam] =
       |  TryAnalysis(transformation(f))""".stripMargin
  }

  val transformations: Helper = transformation && transformationWithAnalysis

  val get: Helper = { (name, typeParameters) =>
    val tParam = stringifyTypeParameters(typeParameters)
    s"""/** Applies an action to the underlying $name. */
       |def get[U](f: Underlying$name$tParam => U): U = f(underlying)""".stripMargin
  }

  val getWithAnalysis: Helper = { (name, typeParameters) =>
    val tParam = stringifyTypeParameters(typeParameters)
    s"""/** Applies an action to the underlying $name, it is used for
       | * transformations that can fail due to an AnalysisException.
       | */
       |def getWithAnalysis[U](f: Underlying$name$tParam => U): TryAnalysis[U] =
       |  TryAnalysis(f(underlying))""".stripMargin
  }

  val gets: Helper = get && getWithAnalysis

  val unpack: Helper = { (name, typeParameters) =>
    val tParam = stringifyTypeParameters(typeParameters)
    s"""/**
       | * Unpack the underlying $name into a DataFrame.
       | */
       |def unpack[U](f: Underlying$name$tParam => UnderlyingDataset[U]): Dataset[U] =
       |  Dataset(f(underlying))""".stripMargin
  }

  val unpackWithAnalysis: Helper = { (name, typeParameters) =>
    val tParam = stringifyTypeParameters(typeParameters)
    s"""/**
       | * Unpack the underlying $name into a DataFrame, it is used for
       | * transformations that can fail due to an AnalysisException.
       | */
       |def unpackWithAnalysis[U](f: Underlying$name$tParam => UnderlyingDataset[U]): TryAnalysis[Dataset[U]] =
       |  TryAnalysis(unpack(f))""".stripMargin
  }

  val unpacks: Helper = unpack && unpackWithAnalysis
}
