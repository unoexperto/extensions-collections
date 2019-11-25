import sbt.Keys._
import sbt._

lazy val commonSettings = Seq(
  name := "collections",
  organization := "com.walkmind.extensions",
  version := "1.0",
  licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")),
  scalaVersion := "2.13.1"
)

lazy val publishSettings = {
  Seq(
    bintrayOrganization := Some("cppexpert"),
    publishArtifact in Test := false,
    publishArtifact := true,

    scmInfo := Some(ScmInfo(url("https://gitlab.com/unoexperto/extensions-collections.git"), "git@gitlab.com:unoexperto/extensions-collections.git")),
    developers += Developer("unoexperto",
      "ruslan",
      "unoexperto.support@mailnull.com",
      url("https://gitlab.com/unoexperto")),
    pomIncludeRepository := (_ => false),
    bintrayPackage := "extensions-collections"
  )
}

lazy val root = (project in file("."))
  .settings(commonSettings: _*)
  .settings(publishSettings: _*)
  .settings(
    compileOrder := CompileOrder.JavaThenScala,
    autoScalaLibrary := false,

    libraryDependencies ++= {
      Seq(
        "org.fusesource.leveldbjni" % "leveldbjni-all" % "1.8" % "provided" withSources(),
        "org.rocksdb" % "rocksdbjni" % "6.3.6" % "provided" withSources(),

        "org.junit.jupiter" % "junit-jupiter-api" % "5.5.2" % Test,
        "io.kotlintest" % "kotlintest-runner-junit5" % "3.4.2" % Test
      )
    },

    // Removing unnecessary dependency
    // https://github.com/pfn/kotlin-plugin/pull/32
    libraryDependencies ~= (_.flatMap { module =>
      if (module.name == "kotlin-scripting-compiler-embeddable")
        Seq.empty
      else
        Seq(module)
    }),

    scalacOptions := Seq("-g:notailcalls", "-release", "8", "-unchecked", "-deprecation", "-encoding", "utf8", "-language:implicitConversions", "-language:postfixOps", "-language:higherKinds", "-Xcheckinit"), //, "-Xlog-implicits"),
    javaOptions in compile ++= Seq("-Xmx2G"),
    javacOptions ++= Seq("-g", "-encoding", "UTF-8", "-source", "8", "-target", "8"), // "--enable-preview"
    crossPaths := false,

    kotlinVersion := "1.3.60",
    kotlincOptions ++= Seq("-verbose", "-jvm-target 1.8"), //,"-XXLanguage:+NewInference" , "-Xinline-classes"),
    kotlinLib("stdlib-jdk8"),

    ivyLoggingLevel := UpdateLogging.Full, // Set Ivy logging to be at the highest level
    logLevel in compile := Level.Warn, // Only show warnings and errors on the screen for compilations. This applies to both test:compile and compile and is Info by default
    logLevel := Level.Warn, // Only show warnings and errors on the screen for all tasks (the default is Info). Individual tasks can then be more verbose using the previous setting
    persistLogLevel := Level.Debug, // Only store messages at info and above (the default is Debug). This is the logging level for replaying logging with 'last'
    traceLevel := 10 // Only show 10 lines of stack traces
  )
  .enablePlugins(DependencyGraphPlugin)
  .enablePlugins(KotlinPlugin)
  .enablePlugins(BintrayPlugin)