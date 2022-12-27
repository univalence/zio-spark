package zio.spark.parameter

import zio.spark.helper._

object SizeSpec
    extends ADTTestFor[Size](
      name = "Size",
      conftests =
        List(
          Conftest("unlimited", unlimitedSize, "0"),
          Conftest("bytes", 2.bytes, "2b"),
          Conftest("bytes (alias)", 2.b, "2b"),
          Conftest("kibibytes", 3.kibibytes, "3kb"),
          Conftest("kibibytes (alias)", 3.kb, "3kb"),
          Conftest("mebibytes", 4.mebibytes, "4mb"),
          Conftest("mebibytes (alias)", 4.mb, "4mb"),
          Conftest("gibibytes", 5.gibibytes, "5gb"),
          Conftest("gibibytes (alias)", 5.gb, "5gb"),
          Conftest("tebibytes", 6.tebibytes, "6tb"),
          Conftest("tebibytes (alias)", 6.tb, "6tb"),
          Conftest("pebibytes", 7.pebibytes, "7pb"),
          Conftest("pebibytes (alias)", 7.pb, "7pb")
        )
    )
