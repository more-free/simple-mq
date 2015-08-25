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
    replay
  }

  def stop = {
    journal.stop
  }

  def replay = {
    journal.replay(maxMemSize) {
        item => item.opcode match {
          case OP_ADD =>
            add(item, true)
          case OP_POP =>
            pop(true)
        }
    }
  }

  def add(data : Array[Byte]) : Unit = {
    add(QItem(OP_ADD, Time.now, data))
  }

  def add(item : QItem, replaying : Boolean = false) : Unit = {
    synchronized {
      if (!isFillingBehind) {
        if(item.data.length + dataSize <= maxMemSize) {
          queue.add(item)
          dataSize += item.data.length
        } else {
          isFillingBehind = true

          if (!replaying) {
            journal.startFillBehind()
          }
        }
      }

      if (!replaying) {
        val future = journal.write(item)
        // force fsync
        future.value
      }
    }
  }

  def pop(replaying :Boolean = false) : Option[QItem] = {
    synchronized {
      if (queue.isEmpty) {
        while (dataSize < maxMemSize && isFillingBehind) {
          journal.read() match {
            case None =>  // end of file, caught up
              isFillingBehind = false
            case Some(item) =>
              if (item.opcode == Protocol.OP_ADD) {
                queue.add(item)
                dataSize += item.data.length
              }
          }
        }
      }

      if (!replaying) {
        val future = journal.write(QItem(OP_POP, Time.now, Array[Byte]()))
        future.value
      }

      if (queue.isEmpty)
        return None

      val top = queue.poll()
      dataSize -= top.data.length
      Some(top)
    }
  }

  def top() : Option[QItem] = {
    synchronized {
      if (queue.isEmpty) None else Some(queue.peek())
    }
  }
}
