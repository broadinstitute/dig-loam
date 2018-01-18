import sbt.project

lazy val Versions = new {
  val App = "1.3-SNAPSHOT"
  val ScalaMajor = "2.12"
  val LoamStreamApp = "1.3-SNAPSHOT"
}

lazy val Orgs = new {
  val DIG = "org.broadinstitute.dig"
}

lazy val Paths = new {
  val LocalRepo = "/humgen/diabetes/users/dig/loamstream/repo"
}

lazy val Resolvers = new {
  val LocalRepo = Resolver.file("localRepo", new File(Paths.LocalRepo))
}

lazy val mainDeps = Seq(
  Orgs.DIG % s"loamstream_${Versions.ScalaMajor}" % Versions.LoamStreamApp classifier "assembly" intransitive()
)

val Gather = config("gather")

libraryDependencies in Gather := mainDeps

lazy val root = (project in file("."))
  .configs(Gather)
  .settings(
    name := "analysisengine-loams",
    organization := Orgs.DIG,
    version := Versions.App,
    libraryDependencies ++= mainDeps,
    publishTo := Some(Resolvers.LocalRepo),
    resolvers ++= Seq(Resolvers.LocalRepo)
  ).settings(
    inConfig(Gather)(Classpaths.ivyBaseSettings): _*
  ).enablePlugins(JavaAppPackaging)

enablePlugins(GitVersioning)

val buildInfoTask = taskKey[Seq[File]]("buildInfo")

buildInfoTask := {
  val dir = (resourceManaged in Compile).value
  val n = name.value
  val v = version.value
  val branch = git.gitCurrentBranch.value
  val lastCommit = git.gitHeadCommit.value
  val describedVersion = git.gitDescribedVersion.value
  val anyUncommittedChanges = git.gitUncommittedChanges.value

  val buildDate = java.time.Instant.now

  val file = dir / "versionInfo.properties"

  val contents = s"name=${n}\nversion=${v}\nbranch=${branch}\nlastCommit=${lastCommit.getOrElse("")}\nuncommittedChanges=${anyUncommittedChanges}\ndescribedVersion=${describedVersion.getOrElse("")}\nbuildDate=${buildDate}\n"

  IO.write(file, contents)

  Seq(file)
}

(resourceGenerators in Compile) += buildInfoTask.taskValue

val gatherDeps = taskKey[Unit]("gatherDeps")

gatherDeps := {
  (update in Gather).value.allFiles.foreach { f =>
    IO.copyFile(f, baseDirectory.value / s"loamstream-${Versions.LoamStreamApp}.jar")
  }
}

