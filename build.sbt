name := "ProvFuzz"
version := "1.0"
scalaVersion := "2.12.10"

resolvers ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots")
)

// lazy val myProject = (project in file("."))
//   .settings(
//     // other settings here...
//     assembly / assemblyExcludedJars := {
//       val cp = (assembly / fullClasspath).value
//       cp.filter { attributed =>
//         attributed.data.getName == "hadoop-hdfs.jar"
//       }
//     }
//   )


//libraryDependencies += "org.json4s" %% "json4s-jackson" % "4.0.3"
libraryDependencies += "org.scalameta" %% "scalameta" % "4.2.3"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.0"
libraryDependencies += "org.roaringbitmap" % "RoaringBitmap" % "0.8.11"
libraryDependencies += "org.scoverage" %% "scalac-scoverage-plugin" % "1.4.1"
libraryDependencies += "org.scoverage" %% "scalac-scoverage-runtime" % "1.4.1"
libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-compiler" % "2.12.10",
  "org.scala-lang" % "scala-reflect" % "2.12.10"
)


libraryDependencies += "com.code-intelligence" % "jazzer-api" % "0.11.0"

// https://mvnrepository.com/artifact/ai.djl/api
libraryDependencies += "ai.djl" % "api" % "0.19.0"
// https://mvnrepository.com/artifact/ai.djl.pytorch/pytorch-engine
libraryDependencies += "ai.djl.pytorch" % "pytorch-engine" % "0.19.0"

libraryDependencies += "io.spray" %%  "spray-json" % "1.3.6"

ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
//  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case PathList("META-INF", "services", "org.apache.hadoop.fs.FileSystem") => MergeStrategy.filterDistinctLines
  case _ => MergeStrategy.first
}

