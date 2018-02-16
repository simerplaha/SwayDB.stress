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

package simulation

import java.util.concurrent.atomic.AtomicInteger

import akka.typed.scaladsl._
import akka.typed.{ActorRef, ActorSystem, Behavior}
import akka.util.Timeout
import base.{CommonAssertions, TestBase}
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{Assertion, Assertions, AsyncWordSpec, BeforeAndAfterAll}
import simulation.Domain.{Product, User}
import simulation.ProductCommand._
import swaydb._
import swaydb.data.accelerate.Accelerator

import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.{Random, Try}
import scala.io.StdIn._

sealed trait ProductCommand
object ProductCommand {
  case object Create extends ProductCommand
  case object Update extends ProductCommand
  case object Delete extends ProductCommand
  case class AssertState(removeAsserted: Boolean) extends ProductCommand
}

class SimulationSpec extends AsyncWordSpec with TestBase with BeforeAndAfterAll with LazyLogging {

  override protected def afterAll(): Unit =
    walkDeleteFolder(dir)

  import swaydb.serializers.Default.IntSerializer

  //  lazy val db = SwayDB.memory[Int, Domain]().assertSuccess
  lazy val db = SwayDB.persistent[Int, Domain](dir, acceleration = Accelerator.brake()).assertSuccess
  //  lazy val db = SwayDB.persistent[Int, Domain](dir, mmapAppendix = false, mmapMaps = false, mmapSegments = MMAP.Disable).assertSuccess

  val ids = new AtomicInteger(0)

  def genId = ids.incrementAndGet()

  override def deleteDB: Boolean = false

  case class UserState(user: User,
                       products: mutable.Set[Product],
                       removedProducts: mutable.Set[Int])

  def userSimulatorActor(state: UserState): Behavior[ProductCommand] = {
    Actor.deferred[ProductCommand] {
      ctx =>
        logger.info(s"${state.user.id}: UserActor started")
        ctx.self ! Create

        Actor.immutable[ProductCommand] {
          case (ctx, message) =>
            val userId = state.user.id
            val self = ctx.self
            //        logger.info(s"Processing message: $message")
            message match {

              case Create =>
                //create 1 product
                val product = Product(genId, randomCharacters() + "_" + 1)
                db.put(product.id, product).assertSuccess
                state.products.add(product)

                //batch Create 2 products
                val batchProduct1 = Product(genId, randomCharacters() + "_" + 1)
                val batchProduct2 = Product(genId, randomCharacters() + "_" + 1)
                db.batchPut(Seq((batchProduct1.id, batchProduct1), (batchProduct2.id, batchProduct2))).assertSuccess
                state.products.add(batchProduct1)
                state.products.add(batchProduct2)

                //do updates and delete every 1000th product added and continue Creating more products
                if (state.products.size >= 1000) {
                  logger.info(s"UserId: $userId - Created 5000 products: ProductId: ${product.id}")
                  self ! AssertState(removeAsserted = false)
                  ctx.schedule(1.second, self, Update)
                  ctx.schedule(2.second, self, Delete)
                  ctx.schedule(3.second, self, AssertState(removeAsserted = true))
                  ctx.schedule(4.second, self, Create)
                } else {
                  self ! Create
                }

                Actor.same

              case Update if state.products.nonEmpty =>
                //single
                logger.info(s"UserId: $userId - Update")
                val createdProducts = Random.shuffle(state.products)

                val product = createdProducts.head
                val updatedProduct = product.copy(name = randomCharacters())
                db.put(product.id, updatedProduct).assertSuccess
                state.products remove product
                state.products add updatedProduct

                //batch
                val batchProduct = createdProducts.takeRight(100)
                val batchUpdatedProducts = batchProduct.map(product => (product.id, product.copy(name = randomCharacters())))
                db.batchPut(batchUpdatedProducts).assertSuccess
                batchProduct.foreach(state.products.remove)
                batchUpdatedProducts.foreach { case (_, product) => state.products.add(product) }
                Actor.same

              case Delete if state.products.nonEmpty =>
                logger.info(s"UserId: $userId - Delete")
                //single delete
                val singleRemove = state.products.head
                db.remove(singleRemove.id).assertSuccess
                state.products remove singleRemove
                state.removedProducts add singleRemove.id

                //batch delete
                val batchProductsToRemove = state.products.takeRight(50)
                val batchRemove = batchProductsToRemove.map(_.id)
                db.batchRemove(batchRemove).assertSuccess
                batchProductsToRemove.foreach {
                  product =>
                    state.products remove product
                    state.removedProducts add product.id
                }
                Actor.same

              case AssertState(removeAsserted) =>
                logger.info(s"UserId: $userId - AssertState. removeAsserted = $removeAsserted")
                db.get(state.user.id).assertSuccess should contain(state.user)
                //assert created products
                val createdProducts = Random.shuffle(state.products)
                logger.info(s"UserId: $userId - start asserting ${createdProducts.size} createdProducts. removeAsserted = $removeAsserted.")
                createdProducts foreach {
                  product =>
                    Try(db.get(product.id).assertSuccess should contain(product)) recoverWith {
                      case ex =>
                        ex.printStackTrace()
                        System.exit(0)
                        throw ex
                    }
                    if (removeAsserted) state.products remove product
                }
                logger.info(s"UserId: $userId - finished asserting ${createdProducts.size} createdProducts. removeAsserted = $removeAsserted.")

                val removedProducts = Random.shuffle(state.removedProducts)
                logger.info(s"UserId: $userId - start asserting ${removedProducts.size} removedProducts. removeAsserted = $removeAsserted.")
                ///assert removed products do not exists
                removedProducts foreach {
                  productId =>
                    Try(db.get(productId).assertSuccess shouldBe empty) recoverWith {
                      case ex =>
                        ex.printStackTrace()
                        System.exit(0)
                        throw ex
                    }
                    if (removeAsserted) state.removedProducts remove productId
                }

                logger.info(s"UserId: $userId - State createdProducts = ${state.products.size} removedProducts = ${state.removedProducts.size}. removeAsserted = $removeAsserted.")
                Actor.same
              case _ =>

                Actor.same
            }
        }
    }
  }

  "Users" should {
    "concurrently Create, Update, Read & Delete (CRUD) Products" in {
      sealed trait TestCommand
      case object StartTest extends TestCommand
      case class EndTest(after: FiniteDuration)(val replyTo: ActorRef[Assertion]) extends TestCommand

      println
      print("Select number of concurrent Users (hit Enter for 100): ")
      val maxUsers: Int = Try(readInt()) getOrElse 100

      print("How many minutes to run the test for (hit Enter for 10 minutes): ")
      val runFor = Try(readInt().minutes) getOrElse 10.minutes

      val guardian =
        Actor.immutable[TestCommand] {
          case (ctx, command) =>
            //create the user
            command match {
              case StartTest =>
                ids.set(maxUsers + 1)
                (1 to maxUsers) map {
                  id =>
                    val user = User(id)
                    db.put(user.id, user).assertSuccess
                    user
                } foreach {
                  user =>
                    //spawn User simulator actor
                    ctx.spawn(userSimulatorActor(UserState(user, mutable.Set(), mutable.Set())), user.id.toString)
                }
              case command @ EndTest(after) =>
                ctx.schedule(after, command.replyTo, Assertions.succeed)
            }
            Actor.same
        }

      val system: ActorSystem[TestCommand] = ActorSystem(guardian, "simulation-test", executionContext = Some(SwayDB.defaultExecutionContext))

      import akka.typed.scaladsl.AskPattern._
      implicit val sc = system.scheduler

      implicit val timeout = Timeout(runFor + 10.seconds)

      system ! StartTest
      (system ? EndTest(runFor)) map {
        assertion =>
          logger.info(
            s"""
               |*****************************************************************
               |****** SIMULATION TEST ENDED AFTER RUNNING FOR $runFor *******
               |*****************************************************************
                  """.stripMargin
          )
          System.exit(0)
          assertion
      }
    }
  }
}