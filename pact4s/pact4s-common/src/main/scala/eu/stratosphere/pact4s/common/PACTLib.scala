package eu.stratosphere.pact4s.common

abstract class PACTProgram {

  case class KeyValuePair[K <% Comparable[K], V](key: K, value: V)

  case class KVPFromValue[V](value: V) {
    def withKey[K <% Comparable[K], V](key: K) = KeyValuePair(key, value)
  }

  case class KVPFromKey[K <% Comparable[K]](key: K) {
    def -->[V](value: V) = KeyValuePair(key, value)
  }

  type WithKey[V, K] = KeyValuePair[K, V]
  type -->[K, V] = KeyValuePair[K, V]

  implicit def value2KVPBuilder[V](value: V) = KVPFromValue(value)
  implicit def key2KVPBuilder[K <% Comparable[K]](key: K) = KVPFromKey(key)

  case class PlanOutputFromSource[K <% Comparable[K], V](source: DataStream[K, V]) {
    def ~>[S](sink: DataSink[K, V, S]) = new PlanOutput(source, sink)
  }

  case class PlanOutputFromSink[K <% Comparable[K], V, S](sink: DataSink[K, V, S]) {
    def <~(source: DataStream[K, V]) = new PlanOutput(source, sink)
  }

  class PlanOutput(source: DataStream[_, _], sink: DataSink[_, _, _])

  implicit def stream2PlanOutputBuilder[K <% Comparable[K], V](source: DataStream[K, V]) = new PlanOutputFromSource(source)
  implicit def sink2PlanOutputBuilder[K <% Comparable[K], V, S](sink: DataSink[K, V, S]) = new PlanOutputFromSink(sink)
  implicit def planOutput2Seq(p: PlanOutput): Seq[PlanOutput] = Seq(p)

  def outputs: Seq[PlanOutput]
  def name: String
  def description: String

  class DataStream[K <% Comparable[K], V] extends Hintable {
    def map[V2](f: (K, V) => V2): DataStream[K, V2] = null
    def map[K2 <% Comparable[K], V2](f: (K, V) => V2 WithKey K2): DataStream[K2, V2] = null
    def cross[K2 <% Comparable[K], V2](that: DataStream[K2, V2]): CrossStream[K, V, K2, V2] = null
    def reduce[V2](f: (K, Iterable[V]) => V2): ReduceStream[K, V2] = null
    def reduce[K2 <% Comparable[K], V2](f: (K, Iterable[V]) => V2 WithKey K2): ReduceStream[K2, V2] = null
    def combine(f: (K, Iterable[V]) => V): CombineStream[K, V] = null
  }
  
  class CrossStream[K1 <% Comparable[K1], V1, K2 <% Comparable[K2], V2] {
    def map[K3 <% Comparable[K3], V3](f: (K1, V1, K2, V2) => V3 WithKey K3): DataStream[K3, V3] = null
    def map[V3](f: (K1, V1, K2, V2) => V3): DataStream[K1, V3] = null
    def flipMap[V3](f: (K2, V2, K1, V1) => V3): DataStream[K2, V3] = null
  }
  
  implicit def cross2Stream[K1 <% Comparable[K1], V1, K2 <% Comparable[K2], V2](cross: CrossStream[K1, V1, K2, V2]) = cross map { (k1: K1, v1: V1, k2: K2, v2: V2) => (v1, k2, v2) }

  class ReduceStream[K <% Comparable[K], V] {
    def map[V2](f: (K, V) => V2): DataStream[K, V2] = null
    def map[K2 <% Comparable[K], V2](f: (K, V) => V2 WithKey K2): DataStream[K2, V2] = null
  }

  class CombineStream[K <% Comparable[K], V] {
    def reduce[V2](f: (K, Iterable[V]) => V2): ReduceStream[K, V2] = null
    def reduce[K2 <% Comparable[K], V2](f: (K, Iterable[V]) => V2 WithKey K2): ReduceStream[K2, V2] = null

    def map[V2](f: (K, V) => V2): DataStream[K, V2] = null
    def map[K2 <% Comparable[K], V2](f: (K, V) => V2 WithKey K2): DataStream[K2, V2] = null
  }

  class DataSource[S, K <% Comparable[K], V](url: String, recordDelimeter: String, parser: S => V WithKey K) extends DataStream[K, V]

  class DataSink[K <% Comparable[K], V, S](url: String, recordDelimeter: String, formatter: (K, V) => S) extends Hintable

  def getHints(item: Hintable): Seq[CompilerHint] = null
  implicit def hint2SeqHint(h: CompilerHint) = Seq(h)

  trait Hintable {
    def unapply(other: Hintable): Boolean = this == other
  }

  class CompilerHint

  case object UniqueKey extends CompilerHint
  case class Degree(degreeOfParallelism: Int) extends CompilerHint
  case class AvgRecordSize(sizeInBytes: Int) extends CompilerHint
  case class Selectivity(selectivityInPercent: Double) extends CompilerHint
}