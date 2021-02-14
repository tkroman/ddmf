package com.tkroman.dd.mf

import utest._

import java.util.UUID

object InMemoryJobServiceTest extends TestSuite {
  override def tests: Tests = Tests {
    test("1 node") {
      val s = new LockingInMemoryJobService(1, 4)
      test("free node, empty queue") {
        val j1 = UUID.randomUUID().toString
        s.submit(j1, 1)
        val status = s.status(j1)
        assert(status == JobStatus.Running)

        val j2 = UUID.randomUUID().toString
        s.submit(j2, 10)
        assert(s.status(j2) == JobStatus.Pending)
        assert(s.status(j1) == JobStatus.Running)

        val j3 = UUID.randomUUID().toString
        s.submit(j3, 10)
        assert(s.status(j3) == JobStatus.Pending)
        assert(s.status(j2) == JobStatus.Pending)
        assert(s.status(j1) == JobStatus.Running)

        val j4 = UUID.randomUUID().toString
        s.submit(j4, 15)
        assert(s.status(j4) == JobStatus.Pending)
        assert(s.status(j3) == JobStatus.Pending)
        assert(s.status(j2) == JobStatus.Pending)
        assert(s.status(j1) == JobStatus.Running)

        s.finish(j1, JobStatus.Succeeded)
        assert(s.status(j4) == JobStatus.Running)
        assert(s.status(j3) == JobStatus.Pending)
        assert(s.status(j2) == JobStatus.Pending)
        assert(s.status(j1) == JobStatus.Succeeded)

        s.finish(j4, JobStatus.Succeeded)
        assert(s.status(j4) == JobStatus.Succeeded)
        assert(s.status(j3) == JobStatus.Pending)
        assert(s.status(j2) == JobStatus.Running)
        assert(s.status(j1) == JobStatus.Succeeded)

        s.finish(j2, JobStatus.Succeeded)
        assert(s.status(j4) == JobStatus.Succeeded)
        assert(s.status(j3) == JobStatus.Running)
        assert(s.status(j2) == JobStatus.Succeeded)
        assert(s.status(j1) == JobStatus.Succeeded)

        s.finish(j3, JobStatus.Succeeded)
        assert(s.status(j4) == JobStatus.Succeeded)
        assert(s.status(j3) == JobStatus.Succeeded)
        assert(s.status(j2) == JobStatus.Succeeded)
        assert(s.status(j1) == JobStatus.Succeeded)
      }
    }
  }
}
