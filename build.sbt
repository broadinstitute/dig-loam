lazy val Orgs = new {
  val DIG = "org.broadinstitute.dig"
}

lazy val Paths = new {
  //`publish` will produce artifacts under this path
  val LocalRepo = "/humgen/diabetes/users/dig/loamstream/repo"
}

lazy val MyResolvers = new {
  val LocalRepo = Resolver.file("localRepo", file(Paths.LocalRepo))
  val SonatypeReleases = Resolver.sonatypeRepo("releases")
  val SonatypeSnapshots = Resolver.sonatypeRepo("snapshots")
  //It would be nice to put the S3 resolver here, but it has to be done inside a macro, like with :=, etc. :\
}

lazy val Buckets = new {
  val digRepo = "dig-repo"
}

lazy val Dependencies = new {
  val digLoamImages = (Orgs.DIG %% "dig-loam-images" % "1.0").artifacts(Artifact("dig-loam-images", "zip", "zip"))
}

//Publish locally (to the Broad FS) and to S3
publishResolvers := Seq[Resolver](
  MyResolvers.LocalRepo,
  {
    val prefix = if (isSnapshot.value) "snapshots" else "releases"

    s3resolver.value(s"${Buckets.digRepo}/${prefix}", s3(s"${Buckets.digRepo}/${prefix}"))
  }
)

//Make it so `publish` publishes to both of the above places.  Without this, we'd have to manually
//multi-publish with `publishAll`.
publish := publishAll.value

// disable publishing the binary jar
publishArtifact in (Compile / packageBin) := false
// disable publishing the javadoc jar
publishArtifact in (Compile / packageDoc) := false
// disable publishing the source jar
publishArtifact in (Compile / packageSrc) := false

// Create an Artifact for publishing a .zip file instead of a .jar
artifact in (Compile / packageBin) := {
  val previous: Artifact = (artifact in (Compile / packageBin)).value

  previous.withType("zip").withExtension("zip")
}

lazy val root = (project in file("."))
  .settings(
    //NB: version set in version.sbt
    name := "dig-loam",
    organization := Orgs.DIG,
    resolvers ++= Seq[Resolver](
      MyResolvers.LocalRepo,
      {
        val prefix = if (isSnapshot.value) "snapshots" else "releases"

        s3resolver.value(s"${Buckets.digRepo}/${prefix}", s3(s"${Buckets.digRepo}/${prefix}"))
      }),
    // add the .zip file to what gets published 
    addArtifact(artifact in (Compile / packageBin), Compile / packageBin).settings,
    libraryDependencies ++= Seq(Dependencies.digLoamImages)
  )

//Make sure the contents of recipes/ makes it into binary artifact
(resourceDirectory in Compile) := baseDirectory.value / "src"

//Enables `buildInfoTask`, which bakes git version info into the jar as the file versionInfo_<project-name>.properties.
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

  val file = dir / s"versionInfo_${n}.properties"

  val contents = s"name=${n}\nversion=${v}\nbranch=${branch}\nlastCommit=${lastCommit.getOrElse("")}\nuncommittedChanges=${anyUncommittedChanges}\ndescribedVersion=${describedVersion.getOrElse("")}\nbuildDate=${buildDate}\n"

  IO.write(file, contents)

  Seq(file)
}

(resourceGenerators in Compile) += buildInfoTask.taskValue
