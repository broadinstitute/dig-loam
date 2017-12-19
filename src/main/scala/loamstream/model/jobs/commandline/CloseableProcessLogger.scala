package loamstream.model.jobs.commandline

import scala.sys.process.ProcessLogger
import loamstream.util.Functions
import loamstream.util.Terminable
import loamstream.util.CanBeClosed

/**
 * @author clint
 * Nov 15, 2017
 */
trait CloseableProcessLogger extends ProcessLogger with Terminable {
  final def close(): Unit = stop()
}
