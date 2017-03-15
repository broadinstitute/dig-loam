package loamstream.apps

import java.nio.file.Path

import loamstream.conf.UgerConfig

import scala.concurrent.duration.Duration
import scala.util.Try
import scala.util.control.NoStackTrace
import org.ggf.drmaa.DrmaaException
import org.scalatest.FunSuite
import loamstream.uger.DrmaaClient
import loamstream.uger.UgerStatus
import loamstream.uger.UgerClient

/**
 * @author clint
 * Oct 19, 2016
 */
final class DrmaaClientHelpersTest extends FunSuite {
  test("Drmaa client is properly shut down") {
    import DrmaaClientHelpersTest._
    
    val ugerClient: UgerClient = UgerClient.useActualBinary()
    
    {
      val helpers = new MockHelpers
      
      assert(helpers.mockClient.isShutdown === false)
      
      helpers.withClient(ugerClient)(client => ())
      
      assert(helpers.mockClient.isShutdown === true)
    }
    
    {
      val helpers = new MockHelpers
      
      assert(helpers.mockClient.isShutdown === false)
      
      try {
        helpers.withClient(ugerClient)(client => throw new Exception)
      } catch {
        case e: Exception => ()
      }
      
      assert(helpers.mockClient.isShutdown === true)
    }
    
    {
      val helpers = new MockHelpers
      
      assert(helpers.mockClient.isShutdown === false)
      
      try {
        helpers.withClient(ugerClient)(client => throw new MockDrmaaException(""))
      } catch {
        case e: Exception => ()
      }
      
      assert(helpers.mockClient.isShutdown === true)
    }
  }
}

object DrmaaClientHelpersTest {
  final class MockDrmaaException(msg: String) extends DrmaaException(msg) with NoStackTrace
  
  final class MockHelpers extends DrmaaClientHelpers {
    val mockClient = new MockDrmaaClient
      
    override private[apps] def makeDrmaaClient(ignored: UgerClient): DrmaaClient = mockClient
  }

  final class MockDrmaaClient extends DrmaaClient {
    var isShutdown = false
      
    override def submitJob(
                            ugerConfig: UgerConfig,
                            pathToScript: Path,
                            jobName: String,
                            numTasks: Int = 1): DrmaaClient.SubmissionResult = ???
    
    override def statusOf(jobId: String): Try[UgerStatus] = ???
  
    override def waitFor(jobId: String, timeout: Duration): Try[UgerStatus] = ???
  
    override def stop(): Unit = {
      isShutdown = true
    }
  }
}
