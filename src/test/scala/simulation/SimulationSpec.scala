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

import akka.typed.scaladsl.AskPattern._
import akka.typed.scaladsl._
import akka.typed.{ActorRef, ActorSystem, Behavior}
import akka.util.Timeout
import base.TestBase
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{Assertion, Assertions, AsyncWordSpec, BeforeAndAfterAll}
import simulation.Domain.{Product, User}
import simulation.ProductCommand._
import simulation.RemoveAsserted.{Remove, RemoveAll}
import swaydb._
import swaydb.data.accelerate.Accelerator

import scala.collection.mutable
import scala.concurrent.duration._
import scala.io.StdIn._
import scala.util.{Random, Try}

sealed trait RemoveAsserted
object RemoveAsserted {
  case class Remove(removeCount: Int) extends RemoveAsserted //removes the input number of Products from the User's state.
  case object RemoveAll extends RemoveAsserted //clear User's state.
  case object RemoveNone extends RemoveAsserted //User's state is not mutated.
}

sealed trait ProductCommand
object ProductCommand {
  case object Create extends ProductCommand
  case object Put extends ProductCommand
  case object Update extends ProductCommand
  case object Expire extends ProductCommand
  case object ExpireRange extends ProductCommand
  case object UpdateRange extends ProductCommand
  case object DeleteRange extends ProductCommand
  case object Delete extends ProductCommand
  case object BatchPut extends ProductCommand
  case object BatchDelete extends ProductCommand
  case object BatchExpire extends ProductCommand
  //assert's User's state and User's products state.
  case class AssertState(removeAsserted: RemoveAsserted) extends ProductCommand
}

class SimulationSpec extends AsyncWordSpec with TestBase with BeforeAndAfterAll with LazyLogging {

  override protected def afterAll(): Unit =
    walkDeleteFolder(dir)

  import swaydb.serializers.Default.IntSerializer

  //select any one of the following database to run test on.
  //  lazy val db = SwayDB.memory[Int, Domain]().assertSuccess
  lazy val db = SwayDB.persistent[Int, Domain](dir, acceleration = Accelerator.brake()).assertSuccess
  //  lazy val db = SwayDB.persistent[Int, Domain](dir, mmapAppendix = false, mmapMaps = false, mmapSegments = MMAP.Disable).assertSuccess

  val ids = new AtomicInteger(0)

  case class UserState(userId: Int,
                       var nextProductId: Int,
                       user: User,
                       products: mutable.Map[Int, (Product, Option[Deadline])],
                       removedProducts: mutable.Set[Int],
                       var productsCreatedCountBeforeAssertion: Int)

  def userSimulatorActor(state: UserState): Behavior[ProductCommand] = {
    Actor.deferred[ProductCommand] {
      ctx =>
        logger.info(s"${state.userId}: UserActor started")
        ctx.self ! Create

        Actor.immutable[ProductCommand] {
          case (ctx, message) =>
            val userId = state.userId
            val self = ctx.self

            def genProductId = {
              val nextId = state.nextProductId
              state.nextProductId = state.nextProductId + 1
              nextId.toInt
            }

            message match {

              case Create =>
                //create 1 product
                val (productId, product) = (genProductId, Product(randomCharacters()))
                db.put(productId, product).assertSuccess
                state.products.put(productId, (product, None))

                //batch Create 2 products
                val (batchProductId1, batchProduct1) = (genProductId, Product(randomCharacters()))
                val (batchProductId2, batchProduct2) = (genProductId, Product(randomCharacters()))
                db.batchPut(Seq((batchProductId1, batchProduct1), (batchProductId2, batchProduct2))).assertSuccess
                state.products.put(batchProductId1, (batchProduct1, None))
                state.products.put(batchProductId2, (batchProduct2, None))

                //increment counter for the 3 created products
                state.productsCreatedCountBeforeAssertion = state.productsCreatedCountBeforeAssertion + 3
                //max number of products to create before asserting the database state for this User's created products.
                val maxProductsToCreateBeforeAssertion = 1000
                //do updates and delete every 1000th product added and continue Creating more products
                if (state.productsCreatedCountBeforeAssertion >= maxProductsToCreateBeforeAssertion) {
                  logger.info(s"UserId: $userId - Created ${state.productsCreatedCountBeforeAssertion} products, state.products = ${state.products.size}, state.removedProducts = ${state.removedProducts.size} - ProductId: $productId")
                  self ! AssertState(removeAsserted = RemoveAsserted.RemoveNone)
                  ctx.schedule((randomNextInt(3) + 1).second, self, Put)
                  ctx.schedule((randomNextInt(3) + 1).second, self, BatchPut)
                  ctx.schedule((randomNextInt(3) + 1).second, self, UpdateRange)
                  ctx.schedule((randomNextInt(3) + 1).second, self, Delete)
                  ctx.schedule((randomNextInt(3) + 1).second, self, Expire)
                  ctx.schedule((randomNextInt(3) + 1).second, self, BatchDelete)
                  ctx.schedule((randomNextInt(3) + 1).second, self, BatchExpire)
                  ctx.schedule((randomNextInt(3) + 1).second, self, DeleteRange)
                  ctx.schedule((randomNextInt(3) + 1).second, self, ExpireRange)
                  //if this User accumulates more then 5000 products in-memory, then assert and remove all
                  if (state.products.size + state.removedProducts.size >= 5000)
                    ctx.schedule(3.second, self, AssertState(removeAsserted = RemoveAsserted.RemoveAll))
                  //if this User accumulates more then 1000 products in-memory, then assert and remove 10
                  else if (state.products.size + state.removedProducts.size >= 1000)
                    ctx.schedule(3.second, self, AssertState(removeAsserted = RemoveAsserted.Remove(10)))
                  //other do not remove any in-memory data.
                  else
                    ctx.schedule(3.second, self, AssertState(removeAsserted = RemoveAsserted.RemoveNone))

                  //also schedule a Create to repeatedly keep creating more Products by this User.
                  ctx.schedule(4.second, self, Create)
                  //reset the counter as the assertion is triggered.
                  state.productsCreatedCountBeforeAssertion = 0
                  logger.info(s"UserId: $userId - Reset created product counter to ${state.productsCreatedCountBeforeAssertion} products")
                } else {
                  //keep on Creating more Products.
                  self ! Create
                }

                Actor.same

              case Put if state.products.nonEmpty =>
                //single
                logger.info(s"UserId: $userId - Put")

                val randomCreatedProducts = Random.shuffle(state.products)

                //put a random existing single product
                val (productId, (product, _)) = randomCreatedProducts.head
                val putProduct = product.copy(name = product.name + "_" + randomCharacters() + "_put")
                db.put(productId, putProduct).assertSuccess
                state.products.put(productId, (putProduct, None))
                Actor.same

              case Update if state.products.nonEmpty =>
                //single
                logger.info(s"UserId: $userId - Update")

                val randomCreatedProducts = Random.shuffle(state.products)

                //update a random single product
                val (productId, (product, deadline)) = randomCreatedProducts.head
                val updatedProduct = product.copy(name = product.name + "_" + randomCharacters() + "_updated")
                db.update(productId, updatedProduct).assertSuccess
                state.products.put(productId, (updatedProduct, deadline))
                Actor.same

              case Expire if state.products.nonEmpty =>
                //single
                logger.info(s"UserId: $userId - Expire")

                val randomCreatedProducts = Random.shuffle(state.products)

                //update a random single product
                val (productId, (product, deadline)) = randomCreatedProducts.head
                val newDeadline = deadline.map(_ - 1.second) getOrElse 1.hour.fromNow
                db.expire(productId, newDeadline).assertSuccess
                state.products.put(productId, (product, Some(newDeadline)))
                Actor.same

              case BatchPut if state.products.nonEmpty =>
                //single
                logger.info(s"UserId: $userId - BatchUpdate")

                val randomCreatedProducts = Random.shuffle(state.products)

                //batch update random 100 products.
                val batchProduct = randomCreatedProducts.takeRight(10)
                val batchUpdatedProducts =
                  batchProduct map {
                    case (productId, (product, _)) =>
                      (productId, product.copy(name = product.name + "_" + randomCharacters() + "_batch_put"))
                  }
                db.batchPut(batchUpdatedProducts).assertSuccess

                batchUpdatedProducts foreach {
                  case (id, product) =>
                    state.products.put(id, (product, None))
                }
                Actor.same

              case UpdateRange if state.products.size >= 60 =>
                logger.info(s"UserId: $userId - UpdateRange")

                val randomCreatedProducts = Random.shuffle(state.products)

                val from = randomCreatedProducts.head._1 min randomCreatedProducts.last._1
                val to = randomCreatedProducts.head._1 max randomCreatedProducts.last._1

                db.update(from, to, Product("range update")).assertSuccess

                (from to to) foreach {
                  updatedProductId =>
                    state.products.get(updatedProductId) map {
                      case (_, deadline) =>
                        state.products.put(updatedProductId, (Product("range update"), deadline))
                    }
                }

                Actor.same

              case ExpireRange if state.products.size >= 60 =>
                logger.info(s"UserId: $userId - ExpireRange")

                val randomCreatedProducts = Random.shuffle(state.products)

                val from = randomCreatedProducts.head._1 min randomCreatedProducts.last._1
                val to = randomCreatedProducts.head._1 max randomCreatedProducts.last._1

                val allDeadlines = state.products.filter(product => product._1 >= from && product._1 <= to).flatMap(_._2._2)
                val newDeadline =
                  if (allDeadlines.nonEmpty)
                    allDeadlines.min - 1.second
                  else
                    1.hour.fromNow

                db.expire(from, to, newDeadline).assertSuccess

                (from to to) foreach {
                  updatedProductId =>
                    state.products.get(updatedProductId) map {
                      case (product, _) =>
                        state.products.put(updatedProductId, (product, Some(newDeadline)))
                    }
                }

                Actor.same

              case Delete if state.products.nonEmpty =>
                logger.info(s"UserId: $userId - Delete")

                val randomCreatedProducts = Random.shuffle(state.products)

                //delete random single product
                val (productToRemoveId, productToRemove) = randomCreatedProducts.head
                db.remove(productToRemoveId).assertSuccess
                state.products remove productToRemoveId
                state.removedProducts add productToRemoveId
                Actor.same

              case BatchDelete if state.products.nonEmpty =>
                logger.info(s"UserId: $userId - BatchDelete")

                val randomCreatedProducts = Random.shuffle(state.products)

                //batch delete random multiple 50 products
                val batchProductsToRemove = randomCreatedProducts.takeRight(50)
                val batchRemove = batchProductsToRemove.map(_._1)
                db.batchRemove(batchRemove).assertSuccess
                batchProductsToRemove foreach {
                  case (productId, _) =>
                    //                    logger.info(s"UserId: $userId - Batch remove product: $productId")
                    state.products remove productId
                    state.removedProducts add productId
                }
                Actor.same

              case BatchExpire if state.products.nonEmpty =>
                logger.info(s"UserId: $userId - BatchExpire")

                val randomCreatedProducts = Random.shuffle(state.products)

                //batch expire random multiple 50 products
                val batchProductsToExpire = randomCreatedProducts.takeRight(50)
                val batchExpire = batchProductsToExpire.map(product => (product._1, product._2._2))
                val allDeadlines = batchExpire.flatMap(_._2)
                val newDeadline =
                  if (allDeadlines.isEmpty)
                    1.hour.fromNow
                  else
                    allDeadlines.min - 1.second

                val expire = batchExpire.map(product => (product._1, newDeadline))

                db.batchExpire(expire).assertSuccess
                batchProductsToExpire foreach {
                  case (productId, (product, _)) =>
                    //                    logger.info(s"UserId: $userId - Batch remove product: $productId")
                    state.products.put(productId, (product, Some(newDeadline)))
                }
                Actor.same

              case DeleteRange if state.products.size >= 60 =>
                logger.info(s"UserId: $userId - RangeDelete")

                val randomCreatedProducts = Random.shuffle(state.products)

                val from = randomCreatedProducts.head._1 min randomCreatedProducts.last._1
                val to = randomCreatedProducts.head._1 max randomCreatedProducts.last._1

                db.remove(from, to).assertSuccess

                (from to to) foreach {
                  removedProductId =>
                    state.products remove removedProductId
                    state.removedProducts add removedProductId
                }
                Actor.same

              case AssertState(removeAsserted) =>
                logger.info(s"UserId: $userId - AssertState. Asserting User.")
                //assert the state of the User itself. This is a static record and does not mutate.
                db.get(state.userId).assertSuccess should contain(state.user)
                val shuffledCreatedProducts = Random.shuffle(state.products)
                logger.info(s"UserId: $userId - start asserting ${shuffledCreatedProducts.size} createdProducts. removeAsserted = $removeAsserted.")
                //assert the state of created products in the User's state and remove products from state if required.
                val removedProducts =
                  shuffledCreatedProducts.foldLeft(0) {
                    case (removeCount, (productId, (product, deadline))) =>
                      Try {
                        db.get(productId).assertSuccess should contain(product)
                        db.expiration(productId).assertSuccess shouldBe deadline
                      } recoverWith {
                        case ex =>
                          System.err.println(s"*************************************************************** 111 At ID: $productId - deadline $deadline")
                          ex.printStackTrace()
                          System.exit(0)
                          throw ex
                      }
                      removeAsserted match {
                        case Remove(maxToRemove) if removeCount < maxToRemove =>
                          state.products remove productId
                          removeCount + 1
                        case RemoveAll =>
                          state.products remove productId
                          removeCount + 1
                        case _ =>
                          removeCount
                      }
                  }
                logger.info(s"UserId: $userId - finished asserting ${shuffledCreatedProducts.size} createdProducts. removedProducts = $removedProducts, removeAsserted = $removeAsserted.")

                val shuffledRemovedProducts = Random.shuffle(state.removedProducts)
                logger.info(s"UserId: $userId - start asserting ${shuffledRemovedProducts.size} removedProducts. removeAsserted = $removeAsserted.")
                //assert the state of removed products in the User's state and remove products from state if required.
                val removedRemovedProducts =
                  shuffledRemovedProducts.foldLeft(0) {
                    case (removeCount, productId) =>
                      Try(db.get(productId).assertSuccess shouldBe empty) recoverWith {
                        case ex =>
                          System.err.println(s"*************************************************************** At ID: $productId")
                          ex.printStackTrace()
                          println(db.get(productId))
                          System.exit(0)
                          throw ex
                      }
                      removeAsserted match {
                        case Remove(maxToRemove) if removeCount < maxToRemove =>
                          state.removedProducts remove productId
                          removeCount + 1
                        case RemoveAll =>
                          state.removedProducts remove productId
                          removeCount + 1

                        case _ =>
                          removeCount
                      }
                  }

                logger.info(s"UserId: $userId - finished asserting ${shuffledRemovedProducts.size} removedProducts. removedProducts = $removedRemovedProducts, removeAsserted = $removeAsserted.")
                logger.info(s"UserId: $userId - after Assertion - state.products = ${state.products.size}, state.removedProducts = ${state.removedProducts.size}, removeAsserted = $removeAsserted.")
                Actor.same
              case _ =>

                Actor.same
            }
        }
    }
  }

  "Users" should {

    "concurrently Create, Update, Read & Delete (CRUD) Products" in {
      //Commands for this test only.
      sealed trait TestCommand
      case object StartTest extends TestCommand
      case class EndTest(after: FiniteDuration)(val replyTo: ActorRef[Assertion]) extends TestCommand

      //Get test inputs
      print("\nSelect number of concurrent Users (hit Enter for 100): ")
      val maxUsers: Int = Try(readInt()) getOrElse 100
      //      val maxUsers: Int = 100

      print("How many minutes to run the test for (hit Enter for 10 minutes): ")
      val runFor = Try(readInt().minutes) getOrElse 10.minutes
      //      val runFor = 10.minutes

      //Create actorSystem's root actor
      val guardian =
        Actor.immutable[TestCommand] {
          case (ctx, command) =>
            command match {
              case StartTest => //starts the test
                (1 to maxUsers) map { //create Users in the database
                  id =>
                    val user = User(s"user-$id")
                    db.put(id, user).assertSuccess
                    (id, user)
                } foreach {
                  case (userId, user) =>
                    //spawn User simulator Actor for the created Users.
                    ctx.spawn(
                      userSimulatorActor(
                        UserState(
                          userId = userId,
                          nextProductId = s"${userId}0000000".toInt,
                          user = user,
                          products = mutable.SortedMap(),
                          removedProducts = mutable.Set(),
                          productsCreatedCountBeforeAssertion = 0)
                      ),
                      name = user.name
                    )
                }
              case command @ EndTest(after) => //hook sends a reply to the Actor after timeout to end the test.
                ctx.schedule(after, command.replyTo, Assertions.succeed)
            }
            Actor.same
        }

      //INITIALISE THE ACTOR SYSTEM
      val system: ActorSystem[TestCommand] = ActorSystem(guardian, "simulation-test", executionContext = Some(SwayDB.defaultExecutionContext))

      implicit val sc = system.scheduler
      implicit val timeout = Timeout(runFor + 10.seconds)

      //START TEST
      system ! StartTest
      (system ? EndTest(runFor)) map { //create a future to stop test after test timeout
        assertion =>
          logger.info(
            s"""
               |*****************************************************************
               |****** SIMULATION TEST ENDED AFTER RUNNING FOR $runFor *******
               |*****************************************************************""".stripMargin
          )
          System.exit(0)
          assertion
      }
    }
  }
}