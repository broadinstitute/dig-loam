package loamstream.model.jobs.commandline

import loamstream.model.jobs.LJob

/**
 * @author clint
 * Dec 7, 2017
 */
trait HasCommandLine extends LJob {
  def commandLineString: String
}
