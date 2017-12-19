package loamstream.model.jobs.commandline

import java.nio.file.Path
import loamstream.util.Loggable
import java.io.Writer
import java.io.FileWriter
import java.io.BufferedWriter
import loamstream.util.Throwables
import loamstream.util.LogContext
import loamstream.util.Files
import java.util.concurrent.atomic.AtomicBoolean
import scala.sys.process.ProcessLogger
import loamstream.model.jobs.OutputStreams
import loamstream.util.CanBeClosed
import loamstream.util.Terminable

/**
 * @author clint
 * Dec 6, 2017
 */
final case class ToFilesProcessLogger(
    stdoutPath: Path,
    stderrPath: Path) extends CloseableProcessLogger with Loggable {
 
  import ToFilesProcessLogger.LazyWriterRef
  
  private val writeToStdOut = new LazyWriterRef(stdoutPath)
  private val writeToStdErr = new LazyWriterRef(stderrPath)
  
  private val delegate = ProcessLogger(fout = writeToStdOut.writeFn, ferr = writeToStdErr.writeFn)
  
  override def stop(): Unit = {
    writeToStdOut.close()
    writeToStdErr.close()
  }
  
  //Methods from ProcessLogger 
  /**
   * Will be called with each line read from the process output stream.
   */
  override def out(s: => String): Unit = delegate.out(s)

  /**
   * Will be called with each line read from the process error stream.
   */
  override def err(s: => String): Unit = delegate.err(s)

  /**
   * If a process is begun with one of these `ProcessBuilder` methods:
   *  {{{
   *    def !(log: ProcessLogger): Int
   *    def !<(log: ProcessLogger): Int
   *  }}}
   *  The run will be wrapped in a call to buffer.  This gives the logger
   *  an opportunity to set up and tear down buffering.  At present the
   *  library implementations of `ProcessLogger` simply execute the body
   *  unbuffered.
   */
  override def buffer[T](f: => T): T = delegate.buffer(f)
}

object ToFilesProcessLogger {
  def apply(outputStreams: OutputStreams): ToFilesProcessLogger = {
    ToFilesProcessLogger(outputStreams.stdout, outputStreams.stderr)
  }
  
  private[commandline] def makeNeededParentDirs(outputFile: Path): Unit = {
    Option(outputFile.normalize.getParent).foreach(Files.createDirsIfNecessary)
  }
  
  private final class LazyWriterRef(dest: Path)(implicit logContext: LogContext) {
    val writeFn: String => Unit = { line =>
      anyWritingDone.set(true)
      
      writer.write(line)
    }
    
    import Throwables.quietly
    
    def close(): Unit = if(anyWritingDone.get) { quietly(s"Error closing Writer for $dest")(writer.close()) }
    
    private[this] val anyWritingDone = new AtomicBoolean
    
    private[this] lazy val writer: Writer = {
      makeNeededParentDirs(dest)
      
      new BufferedWriter(new FileWriter(dest.toFile))
    }
  }
}
