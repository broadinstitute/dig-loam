package loamstream.util

import loamstream.util.snag.Snag
import loamstream.util.shot.{Hit, Miss, Shot}

import scala.reflect.runtime.universe.Type

/**
  * LoamStream
  * Created by oliverr on 1/20/2016.
  */
object ProductTypeExploder {

  val tuplePrefix = "scala.Tuple"

  def unsupportedTypeMiss(typeName: String): Miss =
    Miss(Snag("Don't know how to explode type " + typeName))

  def explode(tpe: Type): Shot[Seq[Type]] = {
    val fullName = tpe.typeConstructor.typeSymbol.fullName
    if (fullName.startsWith(tuplePrefix)) {
      val suffix = fullName.replace(tuplePrefix, "")
      try {
        suffix.toInt
        Hit(tpe.typeArgs)
      } catch {
        case ex: NumberFormatException => unsupportedTypeMiss(fullName)
      }
    } else {
      unsupportedTypeMiss(fullName)
    }
  }

}
