package infra

import java.io.{FileOutputStream, FileInputStream, File}
import java.nio.ByteBuffer
import java.nio.file.{Path, Files, Paths}
import java.util.concurrent.{TimeUnit, Executors, ScheduledExecutorService}

import com.twitter.util.Duration
import com.typesafe.scalalogging.slf4j.Logger
import org.junit.{Assert, After, Before, Test}
import org.slf4j.LoggerFactory

import Protocol._

class TestPersistentQueue {
  val logger = Logger(LoggerFactory.getLogger(this.getClass.getCanonicalName))
  val folderPath = Paths.get(".").toAbsolutePath.normalize().toString + "/.tmp"
  val queuePath = folderPath + "/queue"

  @Before def setUp : Unit = {
    val folder = Paths.get(folderPath)
    if (Files.notExists(folder))
      Files.createDirectory(folder)

    Files.createFile(Paths.get(queuePath))
    logger.debug("creating queue file " + queuePath)
  }

  @After def tearDown : Unit = {
    def deleteFiles(path : Path) : Unit = {
      if (Files.isDirectory(path)) {
        val stream = Files.newDirectoryStream(path)
        val iter = stream.iterator()
        while (iter.hasNext) {
          deleteFiles(iter.next())
        }
        stream.close()
      } else {
        Files.delete(path)
        logger.debug("deleting test file " + path.toString)
      }
    }
    deleteFiles(Paths.get(folderPath))
  }

  @Test def queueOpsWithLimitedMemory = {
    val file = new File(queuePath)
    val scheduler = Executors.newScheduledThreadPool(8)
    val period = Duration(5, TimeUnit.MILLISECONDS)

    val journal = new Journal(file, scheduler, period)
    val queue = new PersistentQueue(12, journal)

    val ints = 1 to 5 toArray
    val data = ints.map {
      int => ByteBuffer.allocate(4).putInt(int).array()
    }

    queue.start

    for (_ <- 1 to 2) { // repeat twice
      data.foreach(d => queue.add(d))

      var id = 0
      for (i <- 0 until ints.length) {
        val num = ByteBuffer.wrap(queue.pop().get.data).getInt()
        Assert.assertEquals(num, ints(id))
        id += 1
      }
      Assert.assertEquals(true, queue.pop() match { case None => true })
    }

    queue.stop
  }

  def recoverFromFailure = {
    var file = new File(queuePath)
    var scheduler = Executors.newScheduledThreadPool(8)
    var period = Duration(5, TimeUnit.MILLISECONDS)
    var journal = new Journal(file, scheduler, period)
    var queue = new PersistentQueue(12, journal)

    queue.start

    val ints = 1 to 5 toArray
    val data = ints.map {
      int => ByteBuffer.allocate(4).putInt(int).array()
    }
    data.foreach(d => queue.add(d))
    queue.pop()

    // close queue and file to simulate a crash
    queue.stop

    // create new queue
    logger.debug("starting to recover")
    file = new File(queuePath)
    scheduler = Executors.newScheduledThreadPool(8)
    period = Duration(5, TimeUnit.MILLISECONDS)
    journal = new Journal(file, scheduler, period)
    queue = new PersistentQueue(12, journal)
    queue.start

    val expected = 2 to 5 toArray
    var id = 0
    while (id < expected.length) {
      val num = ByteBuffer.wrap(queue.pop().get.data).getInt()
      Assert.assertEquals(expected(id), num)
      id += 1
    }
    Assert.assertEquals(true, queue.pop() match { case None => true })
    queue.stop
  }

  @Test def recoverFromFailurePopMore = {
    var file = new File(queuePath)
    var scheduler = Executors.newScheduledThreadPool(8)
    var period = Duration(5, TimeUnit.MILLISECONDS)
    var journal = new Journal(file, scheduler, period)
    var queue = new PersistentQueue(12, journal)

    queue.start

    val ints = 1 to 100 toArray
    val data = ints.map {
      int => ByteBuffer.allocate(4).putInt(int).array()
    }
    data.foreach(d => queue.add(d))
    1 to 50 foreach( _ => queue.pop())

    // close queue and file to simulate a crash
    queue.stop

    // create new queue
    logger.debug("starting to recover")
    file = new File(queuePath)
    scheduler = Executors.newScheduledThreadPool(8)
    period = Duration(5, TimeUnit.MILLISECONDS)
    journal = new Journal(file, scheduler, period)
    queue = new PersistentQueue(12, journal)
    queue.start

    val expected = 51 to 100 toArray
    var id = 0
    while (id < expected.length) {
      val num = ByteBuffer.wrap(queue.pop().get.data).getInt()
      Assert.assertEquals(expected(id), num)
      id += 1
    }
    Assert.assertEquals(true, queue.pop() match { case None => true })
    queue.stop
  }
}
