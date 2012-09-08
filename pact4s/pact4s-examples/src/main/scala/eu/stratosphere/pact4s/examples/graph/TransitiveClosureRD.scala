/**
 * *********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * ********************************************************************************************************************
 */

package eu.stratosphere.pact4s.examples.graph

import scala.math._
import scala.math.Ordered._

import eu.stratosphere.pact4s.common._
import eu.stratosphere.pact4s.common.operators._

/**
 * Transitive Closure with Recursive Doubling
 */
class TransitiveClosureRDDescriptor extends PactDescriptor[TransitiveClosureRD] {
  override val name = "Transitive Closure with Recursive Doubling"
  override val description = "Parameters: [numSubTasks] [vertices] [edges] [output]"
  override def getDefaultParallelism(args: Map[Int, String]) = args.getOrElse(0, "1").toInt

  override def createInstance(args: Map[Int, String]) = new TransitiveClosureRD(args.getOrElse(1, "vertices"), args.getOrElse(2, "edges"), args.getOrElse(3, "output"))
}

class TransitiveClosureRD(verticesInput: String, edgesInput: String, pathsOutput: String) extends PactProgram {

  val vertices = new DataSource(verticesInput, DelimetedDataSourceFormat(parseVertex))
  val edges = new DataSource(edgesInput, DelimetedDataSourceFormat(parseEdge))
  val output = new DataSink(pathsOutput, DelimetedDataSinkFormat(formatOutput))

  val transitiveClosure = createClosure iterate (s0 = vertices distinctBy getEdge, ws0 = edges)

  override def outputs = output <~ transitiveClosure

  def createClosure = (c: DataStream[Path], x: DataStream[Path]) => {

    val cNewPaths = x join c on getTo isEqualTo getFrom map joinPaths
    val c1 = cNewPaths cogroup c on getEdge isEqualTo getEdge map selectShortestDistance

    val xNewPaths = x join x on getTo isEqualTo getFrom map joinPaths
    val x1 = xNewPaths cogroup c1 on getEdge isEqualTo getEdge flatMap excludeKnownPaths

    cNewPaths.hints = PactName("cNewPaths")
    c1.hints = PactName("c1")
    xNewPaths.hints = PactName("xNewPaths")
    x1.hints = PactName("x1")

    (c1, x1)
  }

  def selectShortestDistance = (dist1: Iterator[Path], dist2: Iterator[Path]) => (dist1 ++ dist2) minBy { _.dist }

  def excludeKnownPaths = (x: Iterator[Path], c: Iterator[Path]) => if (c.isEmpty) x else Iterator.empty

  def joinPaths = (p1: Path, p2: Path) => (p1, p2) match {
    case (Path(from, _, dist1), Path(_, to, dist2)) => Path(from, to, dist1 + dist2)
  }

  def getEdge = (p: Path) => (p.from, p.to)
  def getFrom = (p: Path) => p.from
  def getTo = (p: Path) => p.to

  vertices.hints = RecordSize(16) +: PactName("Vertices")
  edges.hints = RecordSize(16) +: PactName("Edges")
  transitiveClosure.hints = PactName("Transitive Closure")
  output.hints = RecordSize(16) +: PactName("Output")

  case class Path(from: Int, to: Int, dist: Int)

  def parseVertex = (line: String) => { val v = line.toInt; Path(v, v, 0) }

  val EdgeInputPattern = """(\d+)\|(\d+)\|""".r

  def parseEdge = (line: String) => line match {
    case EdgeInputPattern(from, to) => Path(from.toInt, to.toInt, 1)
  }

  def formatOutput = (path: Path) => "%d|%d|%d".format(path.from, path.to, path.dist)
}

