package eu.stratosphere.pact4s.compiler.util

object Util {

  def partitionByType[T, S <: T: Manifest](items: List[T]): (List[T], List[S]) = {
    val sClass = implicitly[Manifest[S]].erasure
    val (sAsT, t) = items.partition { item => sClass.isAssignableFrom(item.getClass) }
    (t, sAsT map { _.asInstanceOf[S] })
  }
}
