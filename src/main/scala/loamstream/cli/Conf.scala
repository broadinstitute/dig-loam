package loamstream.cli

import java.nio.file.Path
import java.nio.file.Paths

import org.rogach.scallop.exceptions.ScallopException
import org.rogach.scallop._

import loamstream.util.Loggable
import loamstream.util.Versions

/**
 * Provides a command line interface for LoamStream apps
 * using [[https://github.com/scallop/scallop Scallop]] under the hood
 *
 * @param arguments command line arguments provided by the app user
 * @param exitTheJvmOnValidationError whether or not to exit the whole JVM on any validation errors; setting this
 * to false is useful for tests, so that a validation failure doesn't make SBT exit.  If this is false, a 
 * CliException is thrown instead of invoking 'sys.exit()'.
 */
final case class Conf(
    arguments: Seq[String], 
    exitTheJvmOnValidationError: Boolean = true) extends ScallopConf(arguments) with Loggable {
  
  /** Inform the user about expected usage upon erroneous input/behaviour. */
  override def onError(e: Throwable): Unit = e match {
    case ScallopException(message) =>
      error(message)
      printHelp()
      exitOrThrow(message)
    case ex => super.onError(ex)
  }

  /** In the verify stage, check that files with the supplied paths exist. */
  private def validatePathsExist(paths: ScallopOption[List[Path]]): Unit = {
    paths.toOption.foreach { paths =>
      paths.foreach { path =>
        if (!path.toFile.exists) {
          val msg = s"File at '$path' not found"
          
          error(msg)
          exitOrThrow(msg)
        }
      }
    }
  }

  private def printHelpIfNoArgsAndExit(): Unit = {
    if (arguments.isEmpty) {
      printHelp()
      exitOrThrow("No arguments provided")
    }
  }
  
  private def printVersionInfoAndExitIfNeeded(): Unit = {
    if (version()) {
      println(s"${Versions.load().get.toString}") //scalastyle:ignore regex
      exitOrThrow("version")
    }
  }
  
  private def exitOrThrow(msg: String): Unit = {
    if(exitTheJvmOnValidationError) {
      sys.exit(1)
    } else {
      throw new CliException(msg)
    }
  }

  private val listPathConverter: ValueConverter[List[Path]] = listArgConverter[Path](Paths.get(_))
  
  // TODO: Add version info (ideally from build.sbt)?
  banner("""LoamStream is a genomic analysis stack featuring a high-level language, compiler and runtime engine.
           |Usage: scala loamstream.jar [options] [loam file(s)]
           |       or if you have an executable produced by SBT Native Packager:
           |       loamstream [options] [loam file(s)]
           |Options:
           |""".stripMargin)
  
           
  //Using all default args for `opt` makes it a flag 
  val version: ScallopOption[Boolean] = opt[Boolean](descr = "Print version information and exit")
  
  //Using all default args for `opt` makes it a flag 
  val runEverything: ScallopOption[Boolean] = opt[Boolean](
      descr = "Run every step in the pipeline, even if they've already been run")
  
  //Using all default args for `opt` makes it a flag 
  val dryRun: ScallopOption[Boolean] = opt[Boolean](descr = "Only compile the supplied .loam files, don't run them")

  val conf: ScallopOption[Path] = opt[Path](descr = "Path to config file")

  val backend: ScallopOption[BackendType] = {
    val backendConverter: ValueConverter[BackendType] = singleArgConverter[BackendType](BackendType.byName(_).get)

    opt[BackendType](descr = s"Backend to use: must be one of ${BackendType.values.mkString(", ")}")(backendConverter)
  }

  val loams: ScallopOption[List[Path]] = trailArg[List[Path]](
      descr = "Path(s) to loam script(s)",
      required = false,
      validate = _.nonEmpty)(listPathConverter)

  /**
   * NB: "manually" validate all combinations of args, since the interactions between Scallop validation methods
   * (conflicts, codependent, etc) became unmanageable.
   * --conf is always optional
   * --run-everything is always optional
   * --version trumps everything - if it's present, everythign else is optional
   * --backend and --dry-run are mutually exclusive; both require a non-empty list of loam files
   */
  validateOpt(version, conf, runEverything, loams, backend, dryRun) {
    // If --version is supplied, everything else is unchecked
    case (Some(true), _, _, _, _, _) => Right(Unit)

    //--dry-run and a non-empty list of loam files is valid
    case (_, _, _, Some(files), None, Some(true)) if files.nonEmpty => Right(Unit)
    case (_, _, _, None, None, Some(true)) => Left("Please specify at least one Loam file to compile")

    // --backend with a valid backend type and a non-empty list of loam files is valid
    case (_, _, _, Some(files), Some(_), _) if files.nonEmpty => Right(Unit)
    case (_, _, _, None, Some(_), _) => Left("Please specify at least one Loam file to run")
    case (_, _, _, None, _, Some(true)) => Left("Please specify at least one Loam file to compile")

    case _ => Left("Invalid option/argument combination")
  }
  
  /**
   * NB: This needs to come before the call to verify(), or else we don't fail properly when the path
   * supplied to --conf doesn't exist. Shrug.
   */
  validatePathExists(conf)

  verify()
  
  // The following checks come after verify() since options are lazily built by Scallop
  printHelpIfNoArgsAndExit()
  printVersionInfoAndExitIfNeeded()
  
  validatePathsExist(loams)
}
