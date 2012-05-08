package eu.stratosphere.pact4s.common.stubs

import java.io._

import eu.stratosphere.pact.common.contract.Contract
import eu.stratosphere.nephele.configuration.Configuration

class StubParameters extends Serializable {

  def persist(config: Configuration) = StubParameters.setValue(config, this)
  def persist(contract: Contract) = StubParameters.setValue(contract, this)
}

object StubParameters {
  private val parameterName = "pact4s.stub.parameters"

  def getValue[T <: StubParameters](config: Configuration): T = {

    val classLoader = config.getClassLoader()
    val data = config.getString(parameterName, null)

    val decoder = new sun.misc.BASE64Decoder
    val byteData = decoder.decodeBuffer(data)

    val bais = new ByteArrayInputStream(byteData)

    val ois = new ObjectInputStream(bais) {

      override def resolveClass(desc: ObjectStreamClass): Class[_] = {
        try {
          classLoader.loadClass(desc.getName());
        } catch {
          case e: ClassNotFoundException => super.resolveClass(desc)
        }
      }
    }

    try {
      ois.readObject().asInstanceOf[T]

    } finally {
      ois.close
      bais.close
    }
  }

  def setValue(config: Configuration, parameters: StubParameters): Unit = {

    val baos = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(baos)

    try {
      oos.writeObject(parameters)

      val encoder = new sun.misc.BASE64Encoder
      val data = encoder.encode(baos.toByteArray)

      config.setString(parameterName, data)

    } finally {
      oos.close
      baos.close
    }
  }

  def setValue(contract: Contract, parameters: StubParameters): Unit = setValue(contract.getParameters(), parameters)
}