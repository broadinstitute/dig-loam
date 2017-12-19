package loamstream.model.jobs.commandline

import scala.sys.process.ProcessLogger
import java.nio.file.Path
import java.io.FileWriter
import java.io.BufferedWriter
import java.io.Writer
import loamstream.model.jobs.LJob
import java.nio.file.Paths
import loamstream.util.Loggable
import loamstream.util.Throwables
import loamstream.model.jobs.LogFileNames
import loamstream.util.Files
import loamstream.util.Functions
import loamstream.conf.ExecutionConfig
import java.util.concurrent.atomic.AtomicBoolean
import loamstream.model.jobs.OutputStreams

/**
 * @author clint
 * Nov 13, 2017
 */
object ProcessLoggers {
  def forNamedJob(executionConfig: ExecutionConfig, job: LJob): ToFilesProcessLogger = {
    import executionConfig.outputDir
    
    ToFilesProcessLogger(LogFileNames.outputStreams(job, outputDir))
  }
}
