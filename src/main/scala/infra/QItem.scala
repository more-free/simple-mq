package infra

import java.nio.{ByteOrder, ByteBuffer}
import com.twitter.util.Time
import Protocol._

case class QItem(opcode : Byte, addTime : Time, data : Array[Byte]) {
  def pack : ByteBuffer = {
    val buffer = ByteBuffer.allocate(data.length + QITEM_HEADER_SIZE)
    buffer.order(ByteOrder.LITTLE_ENDIAN)
    buffer.put(opcode)
    buffer.putLong(addTime.inMilliseconds)
    buffer.putInt(data.length)
    buffer.put(data)
    buffer.flip()
    buffer
  }
}

object QItem {
  def unpack(bytes : Array[Byte]) : QItem = {
    val buffer = ByteBuffer.wrap(bytes)
    val opcode = buffer.get
    val addTime = Time.fromMilliseconds(buffer.getLong)
    val dataLength = buffer.getInt
    val data = new Array[Byte](dataLength)
    buffer.get(data)
    QItem(opcode, addTime, data)
  }
}

