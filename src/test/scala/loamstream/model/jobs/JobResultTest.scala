package loamstream.model.jobs

import org.scalatest.FunSuite
import loamstream.util.TypeBox

/**
 * @author clint
 * date: Sep 30, 2016
 */
final class JobResultTest extends FunSuite {
  import JobResult._
  
  // scalastyle:off magic.number
  
  test("isSuccess") {
    assert(Success.isSuccess === true)
    
    assert(CommandResult(0).isSuccess === true)
    
    assert(Failure.isSuccess === false)
    
    assert(CommandResult(1).isSuccess === false)
    assert(CommandResult(-1).isSuccess === false)
    assert(CommandResult(42).isSuccess === false)

    assert(FailureWithException(new Exception).isSuccess === false)
  
    assert(ValueSuccess(42, TypeBox.of[Int]).isSuccess === true)
  }
  
  test("isFailure") {
    assert(Success.isFailure === false)

    assert(CommandResult(0).isFailure === false)

    assert(Failure.isFailure === true)

    assert(CommandResult(1).isFailure === true)
    assert(CommandResult(-1).isFailure === true)
    assert(CommandResult(42).isFailure === true)

    assert(FailureWithException(new Exception).isFailure === true)

    assert(ValueSuccess(42, TypeBox.of[Int]).isFailure === false)
  }
  
  // scalastyle:on magic.number
}