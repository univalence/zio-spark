package zio.spark
import zio.spark.UnavailableMethod.buildMessage

object UnavailableMethod {
  def buildMessage(sparkVersion: String, cause: NoSuchMethodError): String =
    "using Spark version  " + sparkVersion + ": can't found " + cause.getMessage + "\n please check the documentation to verify the method is available in your version"
}

class UnavailableMethod(sparkVersion: String, cause: NoSuchMethodError)
    extends RuntimeException(buildMessage(sparkVersion, cause), cause)
