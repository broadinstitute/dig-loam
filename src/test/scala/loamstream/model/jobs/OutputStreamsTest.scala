package loamstream.model.jobs

import org.scalatest.FunSuite
import loamstream.TestHelpers

/**
 * @author clint
 * Dec 7, 2017
 */
final class OutputStreamsTest extends FunSuite {
  test("apply") {
    import TestHelpers.path
    
    val relativePath0 = path("foo")
    val relativePath1 = path("bar")
    
    assert(relativePath0.isAbsolute === false)
    assert(relativePath1.isAbsolute === false)
    
    val os = OutputStreams(relativePath0, relativePath1)
    
    assert(os.stdout.isAbsolute)
    assert(os.stderr.isAbsolute)
    
    assert(os.stdout === relativePath0.toAbsolutePath)
    assert(os.stderr === relativePath1.toAbsolutePath)
  }
}
