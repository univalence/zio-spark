package zio.spark

import org.apache.spark.sql.Encoder

import zio._
import zio.internal.stacktracer.SourceLocation
import zio.spark.parameter._
import zio.spark.rdd.RDD
import zio.spark.sql._
import zio.spark.sql.implicits._
import zio.test.{assert, TestResult}

import scala.reflect.ClassTag

package object test {
  val defaultSparkSession: SparkSession.Builder =
    SparkSession.builder
      .master(localAllNodes)
      .config("spark.sql.shuffle.partitions", 1)
      .config("spark.ui.enabled", value = false)

  def Dataset[T: Encoder](values: T*)(implicit trace: Trace): SIO[Dataset[T]] = values.toDataset

  def RDD[T: ClassTag](values: T*)(implicit trace: Trace): SIO[RDD[T]] = values.toRDD

  private[test] def assertZIOSparkImpl[A, B](value: SIO[A], codePart: String,
                                             assertionPart: String)(assertion: SparkAssertion[A, B])(implicit
                                                                           trace: Trace,
                                                                           sourceLocation: SourceLocation
  ) = {
    value.flatMap(assertion.f).map { a => SparkAssertion.smartAssert(a, codePart, assertionPart)(assertion.assertion)}
  }

  // TODO
  def assertSpark[A, B](value: A)(assertion: SparkAssertion[A, B]): SIO[TestResult] = ???
    //assertZIOSpark(ZIO.succeed(value))(assertion)

  def assertZIOSpark[A, B](value: SIO[A])(assertion: SparkAssertion[A, B]): SIO[TestResult] =
    macro Macros.assert_impl
}
