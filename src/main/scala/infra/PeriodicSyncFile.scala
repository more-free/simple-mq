package infra

import java.io.{FileOutputStream, File}
import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentLinkedQueue, ScheduledExecutorService, TimeUnit}
import com.twitter.util.Duration
import scala.concurrent.{Promise, Future}

class PeriodicSyncFile(file : File, scheduler : ScheduledExecutorService, period : Duration) {
  val writer = new FileOutputStream(file, true).getChannel
  val promises = new ConcurrentLinkedQueue[Promise[Unit]]()
  val fsyncTask = new FsyncTask()
  @volatile var closed = false

  def start = {
    assert(!closed)

    scheduler.scheduleWithFixedDelay(fsyncTask,
      period.inMilliseconds, period.inMilliseconds, TimeUnit.MILLISECONDS)
  }

  def stop = {
    // gracefully stop
    closed = true

    writer.force(true)
    while (!promises.isEmpty) {
      promises.poll().success()
    }
    writer.close()
  }

  def write(buffer : ByteBuffer) : Future[Unit] = {
    if (closed)
      return Future.successful[Unit]()

    while(buffer.hasRemaining) {
      writer.write(buffer)
    }

    val promise = Promise[Unit]()
    promises.add(promise)
    promise.future
  }

  def position = writer.position()

  class FsyncTask extends Runnable {
    override def run = {
      writer.force(true)
      val curSize = promises.size()
      for (i <- 0 until curSize) {
        promises.poll().success()
      }
    }
  }
}
