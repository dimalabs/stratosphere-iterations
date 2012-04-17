package eu.stratosphere.pact4s.example.relational

import scala.math._
import scala.math.Ordered._

import eu.stratosphere.pact4s.common._

/**
 * The TPC-H is a decision support benchmark on relational data.
 * Its documentation and the data generator (DBGEN) can be found
 * on http://www.tpc.org/tpch/ .
 * The PACT program implements a modified version of the query 3 of
 * the TPC-H benchmark including one join, some filtering and an
 * aggregation.
 * SELECT l_orderkey, o_shippriority, sum(l_extendedprice) as revenue
 *   FROM orders, lineitem
 *   WHERE l_orderkey = o_orderkey
 *     AND o_orderstatus = "X"
 *     AND YEAR(o_orderdate) > Y
 *     AND o_orderpriority LIKE "Z%"
 *   GROUP BY l_orderkey, o_shippriority;
 */
class TPCHQuery3(args: String*) extends PACTProgram {

  val orders = new DataSource(params.ordersInput, params.delimeter, parseOrder)
  val lineItems = new DataSource(params.lineItemsInput, params.delimeter, parseLineItem)
  val output = new DataSink(params.output, params.delimeter, formatOutput)

  val filteredOrders = orders filter { (_, o) => o.status == params.status && o.year >= params.minYear && o.orderPriority.startsWith(params.priority) }
  val prioritizedItems = filteredOrders join lineItems map { (key: Int, o: Order, li: LineItem) => (key, o.shipPriority) --> li.extendedPrice }
  val prioritizedOrders = prioritizedItems combine { (_, items) => items.sum } map { (key: (Int, Int), revenue: Double) => key._1 --> PrioritizedOrder(key._1, key._2, revenue) }

  override def outputs = output <~ prioritizedOrders

  override def name = "TCPH Query 3"
  override def description = "Parameters: [noSubStasks] [orders] [lineItems] [output]"

  val params = new {
    val status = 'F'
    val minYear = 1993
    val priority = "5"

    val delimeter = "\n"
    val numSubTasks = if (args.length > 0) args(0).toInt else 1
    val ordersInput = if (args.length > 1) args(1) else ""
    val lineItemsInput = if (args.length > 2) args(2) else ""
    val output = if (args.length > 3) args(3) else ""
  }

  override def getHints(item: Hintable) = item match {
    case orders() => Degree(params.numSubTasks) +: UniqueKey +: ValuesPerKey(1)
    case lineItems() => Degree(params.numSubTasks) +: ValuesPerKey(4)
    case filteredOrders() => Degree(params.numSubTasks) +: RecordSize(32) +: Selectivity(0.05f) +: ValuesPerKey(1)
    case prioritizedItems() => Degree(params.numSubTasks) +: RecordSize(64) +: ValuesPerKey(4)
    case prioritizedOrders() => Degree(params.numSubTasks) +: RecordSize(64) +: Selectivity(1f) +: ValuesPerKey(1)
    case output() => Degree(params.numSubTasks)
  }

  case class Order(key: Int, status: Char, year: Int, month: Int, day: Int, orderPriority: String, shipPriority: Int)
  case class LineItem(key: Int, extendedPrice: Double)
  case class PrioritizedOrder(key: Int, shipPriority: Int, revenue: Double)

  val OrderInputPattern = """(\d+)|(.)|(\d\d\d\d)-(\d\d)-(\d\d)|(.+)|(\d+)""".r
  val LineItemInputPattern = """(\d+)|(\d+\.\d\d)""".r

  def parseOrder(line: String): Int --> Order = line match {
    case OrderInputPattern(key, status, year, month, day, oPr, sPr) => key.toInt --> Order(key.toInt, status(0), year.toInt, month.toInt, day.toInt, oPr, sPr.toInt)
  }

  def parseLineItem(line: String): Int --> LineItem = line match {
    case LineItemInputPattern(key, price) => key.toInt --> LineItem(key.toInt, price.toDouble)
  }

  def formatOutput(key: Int, item: PrioritizedOrder): String = item match {
    case PrioritizedOrder(_, sPr, revenue) => "%d|%d|%.2f".format(key, sPr, revenue)
  }
}