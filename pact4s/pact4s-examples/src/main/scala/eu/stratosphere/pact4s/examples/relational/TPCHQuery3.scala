package eu.stratosphere.pact4s.examples.relational

import scala.math._
import scala.math.Ordered._

import eu.stratosphere.pact4s.common._
import eu.stratosphere.pact4s.common.operators._

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
class TPCHQuery3Descriptor extends PactDescriptor[TPCHQuery3] {
  override val name = "TCPH Query 3"
  override val description = "Parameters: [numSubTasks] [orders] [lineItems] [output]"
  override def getDefaultParallelism(args: Map[Int, String]) = args.getOrElse(0, "1").toInt

  override def createInstance(args: Map[Int, String]) = new TPCHQuery3(args.getOrElse(1, "orders"), args.getOrElse(2, "lineItems"), args.getOrElse(3, "output"))
}

class TPCHQuery3(ordersInput: String, lineItemsInput: String, ordersOutput: String, status: Char = 'F', minYear: Int = 1993, priority: String = "5") extends PactProgram with TPCHQuery3GeneratedImplicits {

  val orders = new DataSource(ordersInput, DelimetedDataSourceFormat(parseOrder))
  val lineItems = new DataSource(lineItemsInput, DelimetedDataSourceFormat(parseLineItem))
  val output = new DataSink(ordersOutput, DelimetedDataSinkFormat(formatOutput))

  val filteredOrders = orders filter { o => o.status == status && o.year > minYear && o.orderPriority.startsWith(priority) }
  val prioritizedItems = filteredOrders join lineItems on { _.orderId } isEqualTo { _.orderId } map { (o: Order, li: LineItem) => PrioritizedOrder(o.orderId, o.shipPriority, li.extendedPrice) }

  val prioritizedOrders = prioritizedItems groupBy { pi => (pi.orderId, pi.shipPriority) } combine {
    _.reduce { (z, s) => z.copy(revenue = z.revenue + s.revenue) }
  }

  override def outputs = output <~ prioritizedOrders

  orders.hints = UniqueKey({ o: Order => o.orderId }) +: PactName("Orders")
  lineItems.hints = PactName("Line Items")
  output.hints = PactName("Output")
  filteredOrders.hints = RecordSize(32) +: RecordsEmitted(0.05f) +: PactName("Filtered Orders")
  prioritizedItems.hints = RecordSize(64) +: PactName("Prioritized Items")
  prioritizedOrders.hints = RecordSize(64) +: RecordsEmitted(1f) +: PactName("Prioritized Orders")

  case class Order(orderId: Int, status: Char, year: Int, month: Int, day: Int, orderPriority: String, shipPriority: Int)
  case class LineItem(orderId: Int, extendedPrice: Double)
  case class PrioritizedOrder(orderId: Int, shipPriority: Int, revenue: Double)

  val OrderInputPattern = """(\d+)\|[^\|]+\|([^\|])\|[^\|]+\|(\d\d\d\d)-(\d\d)-(\d\d)\|([^\|]+)\|[^\|]+\|(\d+)\|[^\|]+\|""".r
  val LineItemInputPattern = """(\d+)\|[^\|]+\|[^\|]+\|[^\|]+\|[^\|]+\|(\d+\.\d\d)\|[^\|]+\|[^\|]+\|[^\|]\|[^\|]\|[^\|]+\|[^\|]+\|[^\|]+\|[^\|]+\|[^\|]+\|[^\|]+\|""".r

  def parseOrder = (line: String) => line match {
    case OrderInputPattern(orderId, status, year, month, day, oPr, sPr) => Order(orderId.toInt, status(0), year.toInt, month.toInt, day.toInt, oPr, sPr.toInt)
  }

  def parseLineItem = (line: String) => line match {
    case LineItemInputPattern(orderId, price) => LineItem(orderId.toInt, price.toDouble)
  }

  def formatOutput = (item: PrioritizedOrder) => "%d|%d|%.2f".format(item.orderId, item.shipPriority, item.revenue)
}

trait TPCHQuery3GeneratedImplicits { this: TPCHQuery3 =>

  import java.io.ObjectInputStream

  import eu.stratosphere.pact4s.common.analyzer._

  import eu.stratosphere.pact.common.`type`._
  import eu.stratosphere.pact.common.`type`.base._

  implicit val orderSerializer: UDT[Order] = new UDT[Order] {

    override val fieldTypes = Array[Class[_ <: Value]](classOf[PactInteger], classOf[PactInteger], classOf[PactInteger], classOf[PactInteger], classOf[PactInteger], classOf[PactString], classOf[PactInteger])

    override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[Order] {

      private val ix0 = indexMap(0)
      private val ix1 = indexMap(1)
      private val ix2 = indexMap(2)
      private val ix3 = indexMap(3)
      private val ix4 = indexMap(4)
      private val ix5 = indexMap(5)
      private val ix6 = indexMap(6)

      @transient private var w0 = new PactInteger()
      @transient private var w1 = new PactInteger()
      @transient private var w2 = new PactInteger()
      @transient private var w3 = new PactInteger()
      @transient private var w4 = new PactInteger()
      @transient private var w5 = new PactString()
      @transient private var w6 = new PactInteger()

      override def serialize(item: Order, record: PactRecord) = {
        val Order(v0, v1, v2, v3, v4, v5, v6) = item

        if (ix0 >= 0) {
          w0.setValue(v0)
          record.setField(ix0, w0)
        }

        if (ix1 >= 0) {
          w1.setValue(v1)
          record.setField(ix1, w1)
        }

        if (ix2 >= 0) {
          w2.setValue(v2)
          record.setField(ix2, w2)
        }

        if (ix3 >= 0) {
          w3.setValue(v3)
          record.setField(ix3, w3)
        }

        if (ix4 >= 0) {
          w4.setValue(v4)
          record.setField(ix4, w4)
        }

        if (ix5 >= 0) {
          w5.setValue(v5)
          record.setField(ix5, w5)
        }

        if (ix6 >= 0) {
          w6.setValue(v6)
          record.setField(ix6, w6)
        }
      }

      override def deserialize(record: PactRecord): Order = {
        var v0: Int = 0
        var v1: Char = 0
        var v2: Int = 0
        var v3: Int = 0
        var v4: Int = 0
        var v5: String = null
        var v6: Int = 0

        if (ix0 >= 0) {
          record.getFieldInto(ix0, w0)
          v0 = w0.getValue()
        }

        if (ix1 >= 0) {
          record.getFieldInto(ix1, w1)
          v1 = w1.getValue().toChar
        }

        if (ix2 >= 0) {
          record.getFieldInto(ix2, w2)
          v2 = w2.getValue()
        }

        if (ix3 >= 0) {
          record.getFieldInto(ix3, w3)
          v3 = w3.getValue()
        }

        if (ix4 >= 0) {
          record.getFieldInto(ix4, w4)
          v4 = w4.getValue()
        }

        if (ix5 >= 0) {
          record.getFieldInto(ix5, w5)
          v5 = w5.getValue()
        }

        if (ix6 >= 0) {
          record.getFieldInto(ix6, w6)
          v6 = w6.getValue()
        }

        Order(v0, v1, v2, v3, v4, v5, v6)
      }

      private def readObject(in: ObjectInputStream) = {
        in.defaultReadObject()
        w0 = new PactInteger()
        w1 = new PactInteger()
        w2 = new PactInteger()
        w3 = new PactInteger()
        w4 = new PactInteger()
        w5 = new PactString()
        w6 = new PactInteger()
      }
    }
  }

  implicit val lineItemSerializer: UDT[LineItem] = new UDT[LineItem] {

    override val fieldTypes = Array[Class[_ <: Value]](classOf[PactInteger], classOf[PactDouble])

    override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[LineItem] {

      private val ix0 = indexMap(0)
      private val ix1 = indexMap(1)

      @transient private var w0 = new PactInteger()
      @transient private var w1 = new PactDouble()

      override def serialize(item: LineItem, record: PactRecord) = {
        val LineItem(v0, v1) = item

        if (ix0 >= 0) {
          w0.setValue(v0)
          record.setField(ix0, w0)
        }

        if (ix1 >= 0) {
          w1.setValue(v1)
          record.setField(ix1, w1)
        }
      }

      override def deserialize(record: PactRecord): LineItem = {
        var v0: Int = 0
        var v1: Double = 0

        if (ix0 >= 0) {
          record.getFieldInto(ix0, w0)
          v0 = w0.getValue()
        }

        if (ix1 >= 0) {
          record.getFieldInto(ix1, w1)
          v1 = w1.getValue()
        }

        LineItem(v0, v1)
      }

      private def readObject(in: ObjectInputStream) = {
        in.defaultReadObject()
        w0 = new PactInteger()
        w1 = new PactDouble()
      }
    }
  }

  implicit val prioritizedOrderSerializer: UDT[PrioritizedOrder] = new UDT[PrioritizedOrder] {

    override val fieldTypes = Array[Class[_ <: Value]](classOf[PactInteger], classOf[PactInteger], classOf[PactDouble])

    override def createSerializer(indexMap: Array[Int]) = new UDTSerializer[PrioritizedOrder] {

      private val ix0 = indexMap(0)
      private val ix1 = indexMap(1)
      private val ix2 = indexMap(2)

      @transient private var w0 = new PactInteger()
      @transient private var w1 = new PactInteger()
      @transient private var w2 = new PactDouble()

      override def serialize(item: PrioritizedOrder, record: PactRecord) = {
        val PrioritizedOrder(v0, v1, v2) = item

        if (ix0 >= 0) {
          w0.setValue(v0)
          record.setField(ix0, w0)
        }

        if (ix1 >= 0) {
          w1.setValue(v1)
          record.setField(ix1, w1)
        }

        if (ix2 >= 0) {
          w2.setValue(v2)
          record.setField(ix2, w2)
        }
      }

      override def deserialize(record: PactRecord): PrioritizedOrder = {
        var v0: Int = 0
        var v1: Int = 0
        var v2: Double = 0

        if (ix0 >= 0) {
          record.getFieldInto(ix0, w0)
          v0 = w0.getValue()
        }

        if (ix1 >= 0) {
          record.getFieldInto(ix1, w1)
          v1 = w1.getValue()
        }

        if (ix2 >= 0) {
          record.getFieldInto(ix2, w2)
          v2 = w2.getValue()
        }

        PrioritizedOrder(v0, v1, v2)
      }

      private def readObject(in: ObjectInputStream) = {
        in.defaultReadObject()
        w0 = new PactInteger()
        w1 = new PactInteger()
        w2 = new PactDouble()
      }
    }
  }

  implicit def udf1: UDF1[Function1[Iterator[PrioritizedOrder], PrioritizedOrder]] = defaultUDF1IterT[PrioritizedOrder, PrioritizedOrder]
  implicit def udf2: UDF2[Function2[Order, LineItem, PrioritizedOrder]] = defaultUDF2[Order, LineItem, PrioritizedOrder]
  implicit def udf3: FieldSelector[Function1[Order, Boolean]] = defaultFieldSelectorT[Order, Boolean]

  implicit def selOutput: FieldSelector[Function1[PrioritizedOrder, Unit]] = defaultFieldSelectorT[PrioritizedOrder, Unit]
  implicit def selPrioritizedItemsLeft: FieldSelector[Function1[Order, Int]] = getFieldSelector[Order, Int](0)
  implicit def selPrioritizedItemsRight: FieldSelector[Function1[LineItem, Int]] = getFieldSelector[LineItem, Int](0)
  implicit def selPrioritizedOrders: FieldSelector[Function1[PrioritizedOrder, (Int, Int)]] = getFieldSelector[PrioritizedOrder, (Int, Int)](0, 1)
}
