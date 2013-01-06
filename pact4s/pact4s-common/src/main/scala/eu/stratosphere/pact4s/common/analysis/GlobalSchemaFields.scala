package eu.stratosphere.pact4s.common.analysis

import eu.stratosphere.pact.common.util.{ FieldSet => PactFieldSet }

abstract sealed class Field extends Serializable {
  val localPos: Int
  val globalPos: GlobalPos
  var isUsed: Boolean = true
}

case class InputField(val localPos: Int, val globalPos: GlobalPos = new GlobalPos) extends Field
case class OutputField(val localPos: Int, val globalPos: GlobalPos = new GlobalPos) extends Field

class FieldSet[+FieldType <: Field] private (private val fields: Seq[FieldType]) extends Serializable {

  private var globalized: Boolean = false
  def isGlobalized = globalized

  def setGlobalized(): Unit = {
    assert(!globalized, "Field set has already been globalized")
    globalized = true
  }

  def apply(localPos: Int): FieldType = fields.find(_.localPos == localPos).get

  def select(selection: Seq[Int]): FieldSet[FieldType] = {
    val outer = this
    new FieldSet[FieldType](selection map apply) {
      override def setGlobalized() = outer.setGlobalized()
      override def isGlobalized = outer.isGlobalized
      override val pactFieldSet = outer.pactFieldSet
    }
  }

  def toSerializerIndexArray: Array[Int] = fields map {
    case field if field.isUsed => field.globalPos.getValue
    case _                     => -1
  } toArray
  
  def toIndexSet: Set[Int] = fields.filter(_.isUsed).map(_.globalPos.getValue).toSet

  protected val pactFieldSet = new PactFieldSet with Serializable {
    import scala.collection.JavaConversions._

    private def getIndexes = toIndexSet.toSeq.map(i => i: java.lang.Integer)

    override def iterator() = getIndexes.toIterator
    override def size() = getIndexes.length
    override def isEmpty() = getIndexes.isEmpty
    override def contains(value: Object) = getIndexes.contains(value)

    override def add(value: java.lang.Integer): Boolean = throw new UnsupportedOperationException
    override def remove(value: Object): Boolean = throw new UnsupportedOperationException
    override def clear(): Unit = throw new UnsupportedOperationException
    override def clone(): Object = this
  }
}

object FieldSet {

  def newInputSet(numFields: Int): FieldSet[InputField] = new FieldSet((0 until numFields) map { InputField(_) })
  def newOutputSet(numFields: Int): FieldSet[OutputField] = new FieldSet((0 until numFields) map { OutputField(_) })

  def newInputSet[T: UDT](): FieldSet[InputField] = newInputSet(implicitly[UDT[T]].numFields)
  def newOutputSet[T: UDT](): FieldSet[OutputField] = newOutputSet(implicitly[UDT[T]].numFields)

  implicit def toSeq[FieldType <: Field](fieldSet: FieldSet[FieldType]): Seq[FieldType] = fieldSet.fields

  implicit def toPactFieldSet(fieldSet: FieldSet[_]): PactFieldSet = fieldSet.pactFieldSet
}

class GlobalPos extends Serializable {

  private var pos: Either[Int, GlobalPos] = null

  def getValue: Int = pos match {
    case null          => -1
    case Left(index)   => index
    case Right(target) => target.getValue
  }

  def getIndex: Option[Int] = pos match {
    case null | Right(_) => None
    case Left(index)     => Some(index)
  }

  def getReference: Option[GlobalPos] = pos match {
    case null | Left(_) => None
    case Right(target)  => Some(target)
  }

  def isUnknown = pos == null
  def isIndex = (pos != null) && pos.isLeft
  def isReference = (pos != null) && pos.isRight

  def setIndex(index: Int) = {
    assert(pos == null || pos.isLeft, "Cannot convert a position reference to an index")
    pos = Left(index)
  }

  def setReference(target: GlobalPos) = {
    assert(pos == null, "Cannot overwrite a known position with a reference")
    pos = Right(target)
  }
}

object GlobalPos {

  object Unknown {
    def unapply(globalPos: GlobalPos): Boolean = globalPos.isUnknown
  }

  object Index {
    def unapply(globalPos: GlobalPos): Option[Int] = globalPos.getIndex
  }

  object Reference {
    def unapply(globalPos: GlobalPos): Option[GlobalPos] = globalPos.getReference
  }
}
