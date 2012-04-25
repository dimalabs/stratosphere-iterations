package eu.stratosphere.pact4s.common.util

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream

import eu.stratosphere.pact.common.contract.Contract
import eu.stratosphere.nephele.configuration.Configuration

case class WrappedContract(contract: Contract)
case class WrappedConfiguration(config: Configuration)

trait ConfigurableContract { this: WrappedContract =>

  def setParameter[T >: Null](name: String, obj: T) = this.contract.getParameters().setObject(name, obj)
}

trait ConfigurableConfiguration { this: WrappedConfiguration =>

  def setObject[T >: Null](name: String, obj: T) = {

    if (obj == null) {
      this.config.setString(name, null)
      
    } else {

      val baos = new ByteArrayOutputStream
      val oos = new ObjectOutputStream(baos)

      try {
        oos.writeObject(obj)

        val encoder = new sun.misc.BASE64Encoder
        val data = encoder.encode(baos.toByteArray)

        this.config.setString(name, data)
        
      } finally {
        oos.close
        baos.close
      }
    }
  }

  def getObject[T >: Null](name: String): T = {

    val data = this.config.getString(name, null)

    if (data == null) {
      null
      
    } else {

      val decoder = new sun.misc.BASE64Decoder
      val byteData = decoder.decodeBuffer(data)

      val bais = new ByteArrayInputStream(byteData)
      val ois = new ObjectInputStream(bais)

      try {
        ois.readObject().asInstanceOf[T]
        
      } finally {
        ois.close
        bais.close
      }
    }
  }
}