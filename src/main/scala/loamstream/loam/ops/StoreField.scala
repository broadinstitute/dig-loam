package loamstream.loam.ops

/** Describes the field of a record of a Loam store */
trait StoreField[Value] {

  /** The type of record this field is part of */
  type Record <: StoreRecord

  /** Get Some(value) if defined, None else */
  def get(record: Record): Option[Value]

  /** Get value if defined, default else */
  def getOrElse(record: Record, default: => Value): Value = get(record).getOrElse(default)

  /** True if value si defined */
  def isDefined(record: Record): Boolean = get(record).nonEmpty

}
