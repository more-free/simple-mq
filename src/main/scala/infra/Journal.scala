package infra

import java.io._
import java.nio.{ByteOrder, ByteBuffer}
import java.util.concurrent.ScheduledExecutorService
import com.twitter.util.{Time, Duration}
import infra.Protocol.BadItemException
import scala.concurrent.Future
import Protocol._

class Journal(file : File, scheduler : ScheduledExecutorService, period : Duration) {
  val writer = new PeriodicSyncFile(file, scheduler, period)
  val reader = new FileInputStream(file).getChannel
  val header = ByteBuffer.allocate(QITEM_HEADER_SIZE)
  header.order(ByteOrder.LITTLE_ENDIAN)

  def start = {
    writer.start
  }

  def stop = {
    reader.close()
    writer.stop
    scheduler.shutdown()
  }

  def write(item : QItem) : Future[Unit] = {
    writer.write(item.pack)
  }

  def startFillBehind() : Unit = {
    reader.position(writer.position)
  }

  def read() : Option[QItem] = {
    synchronized {
      if (readHeader() < 0)
        return None

      val opcode = header.get()
      val addTime = Time.fromMilliseconds(header.getLong())
      val dataSize = header.getInt()

      val data = readData(dataSize)
      data match {
        case None =>
          throw BadItemException()
        case Some(bytes) =>
          return Some(QItem(opcode, addTime, bytes))
      }
    }
  }

  def replay() : Unit = {
    // TODO
  }

  private def readHeader() : Int = {
    header.clear()
    var x : Int = 0
    do {
      x = reader.read(header)
    } while (header.position() < header.limit() && x >= 0)
    header.flip()
    x
  }

  private def readData(dataSize : Int) : Option[Array[Byte]] = {
    val buffer = ByteBuffer.allocate(dataSize)
    var x : Int = 0
    do {
      x = reader.read(buffer)
    } while (buffer.position() < buffer.limit() && x >= 0)

    if(x >= 0) Some(buffer.array()) else None
  }
}
