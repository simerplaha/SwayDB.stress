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

package base

import org.scalatest.Matchers

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

trait CommonAssertions extends Matchers {

  implicit class GetTryImplicit[T](getThis: Try[T]) {
    def assertSuccess: T =
      getThis match {
        case Failure(exception) =>
          fail(exception)
          System.exit(0)
          throw exception
        case Success(value) =>
          value
      }
  }

  implicit class GetOptionImplicit[T](getThis: Option[T]) {
    def assertGet: T = {
      getThis shouldBe defined
      getThis.get
    }
  }

  implicit class GetTryOptionImplicit[T](tryThis: Try[Option[T]]) {
    def assertGet: T =
      tryThis match {
        case Failure(exception) =>
          fail(exception)
          System.exit(0)
          throw exception

        case Success(value) =>
          value.assertGet
      }

    def assertGetOpt: Option[T] =
      tryThis match {
        case Failure(exception) =>
          fail(exception)
          System.exit(0)
          throw exception

        case Success(value) =>
          value
      }
  }

  implicit class FutureAwait[T](f: => Future[T]) {
    def await(timeout: FiniteDuration): T =
      Await.result(f, timeout)

    def await: T =
      Await.result(f, 1.second)

    def runThis(times: Int): Future[Seq[T]] = {
      println(s"runThis $times times")
      val futures = Range.inclusive(1, times) map (_ => f)
      Future.sequence(futures)
    }
  }

  implicit class FutureAwait2[T](f: => T) {
    def runThisInFuture(times: Int): Future[Seq[T]] = {
      println(s"runThis $times times")
      val futures = Range.inclusive(1, times) map { _ => Future(f) }
      Future.sequence(futures)
    }
  }

  implicit class TimesImplicits(int: Int) {
    def times = int

    def time = int
  }

}
