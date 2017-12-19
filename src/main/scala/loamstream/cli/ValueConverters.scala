package loamstream.cli

import org.rogach.scallop.ValueConverter
import java.net.URI
import scala.util.Try
import java.nio.file.Path
import java.nio.file.Paths

/**
 * @author clint
 * Dec 11, 2017
 */
object ValueConverters {
  type ArgLists = List[(String, List[String])]
  
  type ParseResult[A] = Either[String,Option[A]]
  
  private def isValidGoogleUri(uriString: String): Boolean = {
    def schemeIsGs(s: String): Boolean = s.startsWith("gs://")
    
    def toUriOpt(s: String): Option[URI] = Try(URI.create(s)).toOption
    
    Option(uriString).filter(schemeIsGs).flatMap(toUriOpt).isDefined
  }
  
  object PathOrGoogleUriConverter extends ValueConverter[Either[Path, URI]] {
    //NB: See https://github.com/scallop/scallop/wiki/Custom-converters
    //From there:
    // `parse is a method, that takes a list of arguments to all option invokations:
    // for example, "-a 1 2 -a 3 4 5" would produce List(List(1,2),List(3,4,5)).
    // parse returns Left with error message, if there was an error while parsing
    // if no option was found, it returns Right(None)
    // and if option was found, it returns Right(...)`
    override def parse(argLists: ArgLists): ParseResult[Either[Path, URI]] = argLists match {
      case (_, uriString :: Nil) :: Nil if isValidGoogleUri(uriString) => Right(Some(Right(URI.create(uriString))))
      case (_, pathString :: Nil) :: Nil => Right(Some(Left(Paths.get(pathString))))
      case _ => Right(None) // Whether or not parsing is optional shouldn't need to be defined here. :(
    }
  
    //NB: Cargo-culted from https://github.com/scallop/scallop/wiki/Custom-converters :\
    override val argType = org.rogach.scallop.ArgType.LIST
  }
}
