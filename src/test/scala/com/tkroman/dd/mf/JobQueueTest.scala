package com.tkroman.dd.mf

import com.tkroman.dd.mf.JobStatus.Pending
import utest._

object JobQueueTest extends TestSuite {
  override def tests: Tests = Tests {
    test("enqueueing-dequeueing works") {
      val jq = new JobQueue()
      jq.enqueue(Job("0", 0, Pending))
      jq.enqueue(Job("1", 0, Pending))
      jq.enqueue(Job("2", 5, Pending))
      jq.enqueue(Job("3", 5, Pending))
      jq.enqueue(Job("4", 2, Pending))
      jq.enqueue(Job("5", 2, Pending))
      jq.enqueue(Job("6", 1, Pending))
      jq.enqueue(Job("7", 1, Pending))

      assert(jq.dequeue().get.id == "2")
      assert(jq.dequeue().get.id == "3")
      assert(jq.dequeue().get.id == "4")
      assert(jq.dequeue().get.id == "5")
      assert(jq.dequeue().get.id == "6")
      assert(jq.dequeue().get.id == "7")
      assert(jq.dequeue().get.id == "0")
      assert(jq.dequeue().get.id == "1")
    }
  }
}
