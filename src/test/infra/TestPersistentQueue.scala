package infra

import java.io._
import java.nio.ByteBuffer
import java.util.concurrent.{TimeUnit, Executors, ScheduledExecutorService}

import com.twitter.util.Duration
import org.junit.Test

class TestPersistentQueue {
  @Test def TestAll = {
    val file = new File("/Users/morefree/Development/play/infra/some")
    val scheduler = Executors.newScheduledThreadPool(8)
    val period = Duration(5, TimeUnit.MILLISECONDS)

    val journal = new Journal(file, scheduler, period)
    val queue = new PersistentQueue(12, journal)

    val ints = Array(1, 3, 4, 5, 6, 7, 8)
    val data = ints.map {
      int => ByteBuffer.allocate(4).putInt(int).array()
    }

    queue.start
    data.foreach (d => queue.add(d))

    for(i <- 0 until ints.length) {
      val num = ByteBuffer.wrap(queue.pop().data).getInt()
      println(num)
    }

    queue.stop
  }
}
