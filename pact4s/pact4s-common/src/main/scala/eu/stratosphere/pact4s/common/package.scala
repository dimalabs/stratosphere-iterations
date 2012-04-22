package eu.stratosphere.pact4s

package object common {
  implicit def hint2SeqHint(h: CompilerHint): Seq[CompilerHint] = Seq(h)
}