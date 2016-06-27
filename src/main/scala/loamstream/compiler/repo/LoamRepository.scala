package loamstream.compiler.repo

import java.nio.file.Path

import loamstream.util.Shot

/**
  * LoamStream
  * Created by oliverr on 6/1/2016.
  */
object LoamRepository {
  val defaultPackageName = "loam"
  val fileSuffix = ".loam"
  val defaultEntries = Seq("first", "impute")
  val defaultRepo = ofPackage(defaultPackageName, defaultEntries)

  def ofFolder(path: Path): LoamFolderRepository = LoamFolderRepository(path)

  def ofPackage(packageName: String, entries: Seq[String]): LoamPackageRepository =
    LoamPackageRepository(packageName, entries)

  def ofMap(entries: Map[String, String]): LoamMapRepository = LoamMapRepository(entries)
}

trait LoamRepository {
  def list: Seq[String]

  def get(name: String): Shot[String]

  def find(name: String): Shot[String] = get(name).orElse(get(s"$name.loam"))

  def ++(that: LoamRepository) = that match {
    case LoamComboRepository(repos) => LoamComboRepository(this +: repos)
    case _ => LoamComboRepository(Seq(this, that))
  }
}
