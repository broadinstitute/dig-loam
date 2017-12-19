package loamstream.db.slick

import slick.jdbc.JdbcProfile
import loamstream.util.Loggable
import loamstream.util.Futures
import scala.concurrent.ExecutionContext

/**
 * @author clint
 * Dec 7, 2017
 * 
 * NB: Factored out of SlickLoamDao, which had gotten huge
 */
trait CommonOps extends Loggable {
  def descriptor: DbDescriptor
  
  val driver: JdbcProfile

  import driver.api._
  import driver.backend.DatabaseDef
  import Futures.waitFor
  
  type WriteAction[A] =  driver.ProfileAction[A, NoStream, Effect.Write]
  
  protected[slick] val db: DatabaseDef

  protected[slick] val tables: Tables
  
  protected def log(sqlAction: driver.ProfileAction[_, _, _]): Unit = {
    sqlAction.statements.foreach(s => trace(s"SQL: $s"))
  }
  
  private[slick] object Implicits {
    //TODO: re-evaluate; does this make sense?
    implicit val dbExecutionContext: ExecutionContext = db.executor.executionContext
  }
  
  //TODO: Re-evaluate; block all the time?
  private[slick] def runBlocking[A](action: DBIO[A]): A = waitFor(db.run(action))
}
