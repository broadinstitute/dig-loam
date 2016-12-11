package loamstream.loam.ops.mappers

import loamstream.loam.ops.StoreType.TXT
import loamstream.loam.ops.mappers.TextStoreFieldExtractor.defaultNA
import loamstream.loam.ops.{StoreRecord, TextStore, TextStoreField, TextStoreRecord}

import scala.reflect.runtime.universe.{Type, TypeTag, typeOf}

/** Extracting a given field */
object TextStoreFieldExtractor {
  /** String to use if the value of the field is undefined, and no other default value is provided */
  val defaultNA = "?"
}

/** Extracting a given field of type V from a text store of store type SI */
case class TextStoreFieldExtractor[SI <: TextStore : TypeTag, V](field: TextStoreField[SI, V],
                                                                 defaultString: String = defaultNA)
  extends LoamStoreMapper[SI, TXT] {

  val tpeIn: Type = typeOf[SI]
  val tpeOut: Type = typeOf[TXT]

  override def mapDynamicallyTyped(record: StoreRecord, tpeIn: Type, tpeOut: Type): StoreRecord =
    if (record.isInstanceOf[TextStoreRecord] && tpeIn <:< this.tpeIn && this.tpeOut <:< tpeOut) {
      map(record.asInstanceOf[SI#Record])
    } else {
      TextStoreRecord(defaultString)
    }


  /** Map a record */
  override def map(record: SI#Record): TextStoreRecord = {
    TextStoreRecord(field.fieldTextExtractor(record.text).getOrElse(defaultString))
  }
}