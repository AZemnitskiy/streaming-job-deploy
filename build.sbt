name := "Deploy"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies += "com.typesafe" % "config" % "1.3.4"

libraryDependencies +=  "org.scalaj" %% "scalaj-http" % "2.4.1"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"

libraryDependencies += "com.persist" % "persist-json_2.12" % "1.2.0"

test in assembly := {}

assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")            => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}


