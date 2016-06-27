package loamstream.compiler.repo

import loamstream.util.{Shot, Shots}

/**
  * LoamStream
  * Created by oliverr on 6/27/2016.
  */
case class LoamComboRepository(repos: Seq[LoamRepository]) extends LoamRepository {
  override def list: Seq[String] = repos.flatMap(_.list)

  override def get(name: String): Shot[String] = Shots.findHit[LoamRepository, String](repos, _.get(name))

  override def ++(that: LoamRepository) = that match {
    case LoamComboRepository(thoseRepos) => LoamComboRepository(repos ++ thoseRepos)
    case _ => LoamComboRepository(repos :+ that)
  }

  override def add(name: String, content: String): Shot[String] =
    Shots.findHit[LoamRepository, String](repos, _.add(name, content))
}
