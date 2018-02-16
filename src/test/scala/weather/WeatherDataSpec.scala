/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
 *
 * This file is a part of SwayDB.
 *
 * SwayDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * SwayDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package weather

import base.TestBase
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{BeforeAndAfterAll, WordSpec}
import swaydb.SwayDB
import swaydb.core.util.Benchmark
import swaydb.data.accelerate.Accelerator

import scala.concurrent.Future
import scala.concurrent.duration._

import swaydb.order.KeyOrder.default
import swaydb.serializers.Default._

class WeatherDataSpec extends WordSpec with TestBase with LazyLogging with Benchmark with BeforeAndAfterAll {

  override protected def afterAll(): Unit =
    walkDeleteFolder(dir)

  implicit val ec = SwayDB.defaultExecutionContext

  //  val db = SwayDB.memory[Int, WeatherData]().assertSuccess
  val db = SwayDB.persistent[Int, WeatherData](dir, acceleration = Accelerator.brake()).assertSuccess
  //    val db = SwayDB.memoryPersistent[Int, WeatherData](testDir, maxOpenSegments = 10, cacheSize = 10.mb, maxMemoryLevelSize = 1.mb).assertSuccess

  val keyValueCount = 1000000

  def doPut =
    (1 to keyValueCount) foreach {
      key =>
        if (key % 10000 == 0)
          println(s"Put: Key = $key.")
        db.put(key, WeatherData(Water(key, Direction.East, key), Wind(key, Direction.West, key, key), Location.Sydney)).assertSuccess
    }

  //batch writes all key-values inBatchesOf
  def doBatch(inBatchesOf: Int) =
    (1 to keyValueCount) foreach {
      key =>
        if (key % 10000 == 0)
          println(s"Batch: Key = $key.")

        if (key % inBatchesOf == 0) {
          val keyValues =
            (key - (inBatchesOf - 1) to key).map {
              key =>
                (key, WeatherData(Water(key, Direction.East, key), Wind(key, Direction.West, key, key), Location.Sydney))
            }
          db.batchPut(keyValues).assertSuccess
        }
    }

  def doBatchRandom = {
    val from = randomNextInt(keyValueCount) min (keyValueCount - 100)
    val keyValues =
      (from to from + 10) map {
        key =>
          if (key % 10000 == 0)
            println(s"Batch random: Key = $key.")
          (key, WeatherData(Water(key, Direction.East, key), Wind(key, Direction.West, key, key), Location.Sydney))
      }
    db.batchPut(keyValues).assertSuccess
  }

  def doGet =
    (1 to keyValueCount) foreach {
      key =>
        val value = db.get(key).assertSuccess
        if (key % 10000 == 0)
          println(s"Get: Key = $key. Value = $value")
        value should contain(WeatherData(Water(key, Direction.East, key), Wind(key, Direction.West, key, key), Location.Sydney))
    }

  def doFoldLeft =
    db
      .foldLeft(Option.empty[Int]) {
        case (previousKey, (key, value)) =>
          previousKey map {
            previousKey =>
              if (key % 10000 == 0)
                println(s"FoldLeft: previousKey: $previousKey == key = $key. Value = $value")
              key shouldBe (previousKey + 1)
              value shouldBe WeatherData(Water(key, Direction.East, key), Wind(key, Direction.West, key, key), Location.Sydney)
              key
          } orElse Some(key)
      } should contain(keyValueCount)

  def doForeach =
    db
      .foreach {
        case (key, value) =>
          if (key % 10000 == 0)
            println(s"Foreach: key = $key. Value = $value")
          value shouldBe WeatherData(Water(key, Direction.East, key), Wind(key, Direction.West, key, key), Location.Sydney)
      }

  def doTakeWhile = {
    //start from anywhere but take at least 100 keyValues
    val startFrom = randomNextInt(keyValueCount) min (keyValueCount - 100)
    val took =
      db
        .from(startFrom)
        .takeWhile {
          case (key, _) =>
            if (key % 10000 == 0)
              println(s"doTakeWhile: key = $key")
            key < (startFrom + 100)
        }

    took should have size 100
    took.head._1 shouldBe startFrom
    took.last._1 shouldBe (startFrom + 100 - 1)
  }

  def doHeadAndLast = {
    val (headKey, headValue) = db.head
    headKey shouldBe 1
    headValue shouldBe WeatherData(Water(headKey, Direction.East, headKey), Wind(headKey, Direction.West, headKey, headKey), Location.Sydney)
    println(s"headKey: $headKey -> headValue: $headValue")

    val (lastKey, lastValue) = db.last
    lastKey shouldBe keyValueCount
    lastValue shouldBe WeatherData(Water(lastKey, Direction.East, lastKey), Wind(lastKey, Direction.West, lastKey, lastKey), Location.Sydney)
    println(s"lastKey: $lastKey -> lastValue: $lastValue")
  }

  def doMapRight = {
    //start from anywhere but take at least 100 keyValues
    val startFrom = randomNextInt(keyValueCount) min (keyValueCount - 100)
    val took =
      db
        .from(startFrom)
        .until {
          case (key, _) =>
            key > startFrom - 100
        }
        .mapRight {
          case (key, _) =>
            if (key % 10000 == 0)
              println(s"mapRight: key = $key")
            key
        }

    val expected = (0 until 100) map (startFrom - _)
    took should have size 100
    took shouldBe expected
  }

  def doTake = {
    db
      .take(100)
      .map {
        case (key, value) =>
          if (key % 10000 == 0)
            println(s"take: key = $key")
          key
      } shouldBe (1 to 100)

    db
      .fromOrAfter(0)
      .take(100)
      .map {
        case (key, value) =>
          if (key % 10000 == 0)
            println(s"take: key = $key")
          key
      } shouldBe (1 to 100)
  }

  def doDrop =
    db
      .from(keyValueCount - 200)
      .drop(100)
      .map {
        case (key, value) =>
          if (key % 10000 == 0)
            println(s"take: key = $key")
          key
      } shouldBe (keyValueCount - 100 to keyValueCount)

  def doTakeRight =
    db
      .fromOrBefore(Int.MaxValue)
      .takeRight(100)
      .map {
        case (key, value) =>
          if (key % 10000 == 0)
            println(s"take: key = $key")
          key
      } shouldBe (keyValueCount - 99 to keyValueCount).reverse

  def doCount =
    db.size should be >= keyValueCount

  def doDeleteAll =
    (1 to keyValueCount) foreach {
      key =>
        if (key % 10000 == 0)
          println(s"Remove: Key = $key.")
        db.remove(key).assertSuccess
    }

  def putRequest = Future(doPut)

  def batchRandomRequest = Future(doBatchRandom)

  def batchRequest(inBatchesOf: Int = 100) = Future(inBatchesOf)

  def readRequests =
    Future.sequence(
      Seq(
        Future(doForeach),
        Future(doGet),
        Future(doHeadAndLast),
        Future(doFoldLeft),
        Future(doTakeWhile),
        Future(doMapRight),
        Future(doTake),
        Future(doDrop),
        Future(doTakeRight),
        Future(doCount)
      )
    )

  "concurrently write 1 million weather data entries using BookPickle and read using multiple APIs concurrently" in {
    //do initial put or batch (whichever one) to ensure that data exists for readRequests.
    doPut
    //    doBatch(inBatchesOf = 100000 min keyValueCount)
    putRequest runThis 2.times
    batchRandomRequest runThis 2.times
    batchRequest(inBatchesOf = 10000 min keyValueCount)
    readRequests runThis 2.times await 10.minutes
    doDeleteAll
    println("************************* DONE *************************")
  }
}
