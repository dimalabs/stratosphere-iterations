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
class TPCHQuery3(args: String*) extends PactProgram {

  val orders = new DataSource(params.ordersInput, parseOrder)
  val lineItems = new DataSource(params.lineItemsInput, parseLineItem)
  val output = new DataSink(params.output, formatOutput)

  val filteredOrders = orders filter { o => o.status == params.status && o.year >= params.minYear && o.orderPriority.startsWith(params.priority) }
  val prioritizedItems = filteredOrders join lineItems on { _.orderId } isEqualTo { _.orderId } map { (o: Order, li: LineItem) => PrioritizedOrder(o.orderId, o.shipPriority, li.extendedPrice) }
  val prioritizedOrders = prioritizedItems groupBy { pi => (pi.orderId, pi.shipPriority) } combine { items => PrioritizedOrder(items.head.orderId, items.head.shipPriority, items map { _.revenue } sum) }

  override def outputs = output <~ prioritizedOrders

  override def name = "TCPH Query 3"
  override def description = "Parameters: [noSubStasks] [orders] [lineItems] [output]"
  override def defaultParallelism = params.numSubTasks

  orders.hints = UniqueKey
  lineItems.hints = ValuesPerKey(4)
  filteredOrders.hints = RecordSize(32) +: Selectivity(0.05f) +: ValuesPerKey(1)
  prioritizedItems.hints = RecordSize(64) +: ValuesPerKey(4)
  prioritizedOrders.hints = RecordSize(64) +: Selectivity(1f) +: ValuesPerKey(1)
  
  val params = new {
    val status = 'F'
    val minYear = 1993
    val priority = "5"

    val numSubTasks = args(0).toInt
    val ordersInput = args(1)
    val lineItemsInput = args(2)
    val output = args(3)
  }

  case class Order(orderId: Int, status: Char, year: Int, month: Int, day: Int, orderPriority: String, shipPriority: Int)
  case class LineItem(orderId: Int, extendedPrice: Double)
  case class PrioritizedOrder(orderId: Int, shipPriority: Int, revenue: Double)

  val OrderInputPattern = """(\d+)|(.)|(\d\d\d\d)-(\d\d)-(\d\d)|(.+)|(\d+)""".r
  val LineItemInputPattern = """(\d+)|(\d+\.\d\d)""".r

  def parseOrder(line: String): Order = line match {
    case OrderInputPattern(orderId, status, year, month, day, oPr, sPr) => Order(orderId.toInt, status(0), year.toInt, month.toInt, day.toInt, oPr, sPr.toInt)
  }

  def parseLineItem(line: String): LineItem = line match {
    case LineItemInputPattern(orderId, price) => LineItem(orderId.toInt, price.toDouble)
  }

  def formatOutput(item: PrioritizedOrder): String = item match {
    case PrioritizedOrder(orderId, sPr, revenue) => "%d|%d|%.2f".format(orderId, sPr, revenue)
  }
}