package loamstream

import loamstream.LEnv.{Key, KeyBase, LComboEnv}
import loamstream.loam.LEnvBuilder
import loamstream.model.LId
import loamstream.util.{Shot, Snag, TypeBox}

import scala.reflect.runtime.universe.{Type, TypeTag}

/**
  * LoamStream
  * Created by oliverr on 3/30/2016.
  */
trait LEnv {
  def keys: Iterable[KeyBase]

  def size: Int = keys.size

  def apply[V](key: Key[V]): V

  def get[V](key: Key[V]): Option[V]

  def shoot[V](key: Key[V], name: String): Shot[V] = Shot.fromOption(get(key), Snag(s"No value for key '$name'."))

  def grab(key: KeyBase): Option[Any]

  def +[V](key: Key[V], value: V): LEnv

  def +[V](entry: (Key[V], V)): LEnv = this. +(entry._1, entry._2)

  def ++(oEnv: LEnv): LComboEnv = oEnv match {
    case LComboEnv(oEnvs) => LComboEnv(this +: oEnvs)
    case _ => LComboEnv(this, oEnv)
  }
}

object LEnv {

  trait KeyBase {
    def id: LId

    def tpe: Type
  }

  type EntryBase = (KeyBase, Any)
  type Entry[V] = (Key[V], V)

  final case class Key[V](typeBox: TypeBox[V], id: LId) extends KeyBase {
    def tpe: Type = typeBox.tpe

    def apply(value: V): Entry[V] = new Entry(this, value)

    def ->(value: V): Entry[V] = new Entry(this, value) // TODO remove this - ambiguous!

    def :=(value: V)(implicit envBuilder: LEnvBuilder): Entry[V] = {
      val entry = new Entry(this, value)
      envBuilder += entry
      entry
    }
  }

  object Key {
    def create[T: TypeTag]: Key[T] = apply[T](LId.newAnonId)

    def apply[T: TypeTag](id: LId): Key[T] = Key[T](TypeBox.of[T], id)
  }

  def empty: LEnv = LMapEnv(Map.empty)

  def apply(entry: EntryBase, entries: EntryBase*): LMapEnv = LMapEnv((entry +: entries).toMap)

  final case class LMapEnv(entries: Map[KeyBase, Any]) extends LEnv {
    override def keys: Set[KeyBase] = entries.keySet

    override def apply[V](key: Key[V]): V = entries(key).asInstanceOf[V]

    override def get[V](key: Key[V]): Option[V] = entries.get(key).map(_.asInstanceOf[V])

    override def +[V](key: Key[V], value: V): LEnv = copy(entries = entries + (key -> value))

    override def grab(key: KeyBase): Option[Any] = entries.get(key)
  }

  object LComboEnv {
    def apply(env: LEnv, envs: LEnv*): LComboEnv = LComboEnv(env +: envs)
  }

  final case class LComboEnv(envs: Seq[LEnv]) extends LEnv {
    override def keys: Iterable[KeyBase] = envs.flatMap(_.keys)

    override def apply[V](key: Key[V]): V = envs.flatMap(_.get(key)).head

    override def get[V](key: Key[V]): Option[V] = envs.flatMap(_.get(key)).headOption

    override def +[V](key: Key[V], value: V): LEnv = envs.head match {
      case mapEnv: LMapEnv => LComboEnv((mapEnv + (key -> value)) +: envs.tail)
      case _ => LComboEnv(LMapEnv(Map(key -> value)) +: envs)
    }

    override def ++(oEnv: LEnv): LComboEnv = oEnv match {
      case LComboEnv(oEnvs) => LComboEnv(envs ++ oEnvs)
      case _ => LComboEnv(envs :+ oEnv)
    }

    override def grab(key: KeyBase): Option[Any] = envs.flatMap(_.grab(key)).headOption
  }

}
