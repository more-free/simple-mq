package infra

import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentLinkedQueue
import Protocol._
import com.twitter.util.Time

class PersistentQueue(maxMemSize : Long, journal: Journal) {
  val queue = new ConcurrentLinkedQueue[QItem]()
  var dataSize = 0L
  @volatile var isFillingBehind = false

  def start = {
    journal.start
  }

  def stop = {
    journal.stop
  }

  def add(data : Array[Byte]) : Unit = {
    add(QItem(OP_ADD, Time.now, data))
  }

  def add(item : QItem) : Unit = {
    synchronized {
      if (!isFillingBehind) {
        if(item.data.length + dataSize <= maxMemSize) {
          queue.add(item)
          dataSize += item.data.length
        } else {
          isFillingBehind = true
          journal.startFillBehind()
        }
      }

      val future = journal.write(item)
      // force fsync
      future.value
    }
  }

  def pop() : QItem = {
    synchronized {
      if (queue.isEmpty) {
        while (dataSize < maxMemSize && isFillingBehind) {
          journal.read() match {
            case None =>  // end of file, caught up
              isFillingBehind = false
            case Some(item) =>
              queue.add(item)
              dataSize += item.data.length
          }
        }
      }

      val future = journal.write(QItem(OP_POP, Time.now, Array[Byte]()))
      future.value

      val top = queue.poll()
      dataSize -= top.data.length
      top
    }
  }

  def top() : QItem = {
    synchronized {
      queue.peek()
    }
  }
}
