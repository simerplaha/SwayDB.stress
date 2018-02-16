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
import akka.typed.scaladsl.AskPattern._

sealed trait RemoveAsserted
object RemoveAsserted {
  case class Remove(removeCount: Int) extends RemoveAsserted //removes the input number of Products from the User's state.
  case object RemoveAll extends RemoveAsserted //clear User's state.
  case object RemoveNone extends RemoveAsserted //User's state is not mutated.
}

sealed trait ProductCommand
object ProductCommand {
  case object Create extends ProductCommand //Create's products using put and batch
  case object Update extends ProductCommand //Update's products using put and batch
  case object Delete extends ProductCommand //Delete's products using put and batch
  //assert's User's state and User's products state.
  case class AssertState(removeAsserted: RemoveAsserted) extends ProductCommand
}

class SimulationSpec extends AsyncWordSpec with TestBase with BeforeAndAfterAll with LazyLogging {

  override protected def afterAll(): Unit =
    walkDeleteFolder(dir)

  import swaydb.serializers.Default.IntSerializer

  //select any one of the following database to run test on.
  //    lazy val db = SwayDB.memory[Int, Domain]().assertSuccess
  lazy val db = SwayDB.persistent[Int, Domain](dir, acceleration = Accelerator.brake()).assertSuccess
  //  lazy val db = SwayDB.persistent[Int, Domain](dir, mmapAppendix = false, mmapMaps = false, mmapSegments = MMAP.Disable).assertSuccess

  val ids = new AtomicInteger(0)

  def genId = ids.incrementAndGet()

  //  override def deleteDB: Boolean = false

  case class UserState(user: User,
                       products: mutable.Set[Product],
                       removedProducts: mutable.Set[Int],
                       var productsCreatedCountBeforeAssertion: Int)

  def userSimulatorActor(state: UserState): Behavior[ProductCommand] = {
    Actor.deferred[ProductCommand] {
      ctx =>
        logger.info(s"${state.user.id}: UserActor started")
        ctx.self ! Create

        Actor.immutable[ProductCommand] {
          case (ctx, message) =>
            val userId = state.user.id
            val self = ctx.self
            message match {

              case Create =>
                //create 1 product
                val product = Product(genId, randomCharacters())
                db.put(product.id, product).assertSuccess
                state.products.add(product)

                //batch Create 2 products
                val batchProduct1 = Product(genId, randomCharacters())
                val batchProduct2 = Product(genId, randomCharacters())
                db.batchPut(Seq((batchProduct1.id, batchProduct1), (batchProduct2.id, batchProduct2))).assertSuccess
                state.products.add(batchProduct1)
                state.products.add(batchProduct2)

                //increment counter for the 3 created products
                state.productsCreatedCountBeforeAssertion = state.productsCreatedCountBeforeAssertion + 3
                //max number of products to create before asserting the database state for this User's created products.
                val maxProductsToCreateBeforeAssertion = 1000
                //do updates and delete every 1000th product added and continue Creating more products
                if (state.productsCreatedCountBeforeAssertion >= maxProductsToCreateBeforeAssertion) {
                  logger.info(s"UserId: $userId - Created ${state.productsCreatedCountBeforeAssertion} products, state.products = ${state.products.size}, state.removedProducts = ${state.removedProducts.size} - ProductId: ${product.id}")
                  self ! AssertState(removeAsserted = RemoveAsserted.RemoveNone)
                  ctx.schedule(1.second, self, Update) //do update
                  ctx.schedule(2.second, self, Delete) //do delete
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

              case Update if state.products.nonEmpty =>
                //single
                logger.info(s"UserId: $userId - Update")

                def randomCreatedProducts = Random.shuffle(state.products)

                //update a random single product
                val product = randomCreatedProducts.head
                val updatedProduct = product.copy(name = product.name + "_" + randomCharacters() + "_updated")
                db.put(product.id, updatedProduct).assertSuccess
                state.products remove product
                state.products add updatedProduct

                //batch update random 100 products.
                val batchProduct = randomCreatedProducts.takeRight(100)
                val batchUpdatedProducts = batchProduct.map(product => (product.id, product.copy(name = product.name + "_" + randomCharacters() + "_updated")))
                db.batchPut(batchUpdatedProducts).assertSuccess
                batchProduct foreach state.products.remove
                batchUpdatedProducts foreach { case (_, product) => state.products add product }

                Actor.same

              case Delete if state.products.nonEmpty =>
                logger.info(s"UserId: $userId - Delete")

                def randomCreatedProducts = Random.shuffle(state.products)

                //delete random single product
                val singleRemove = randomCreatedProducts.head
                db.remove(singleRemove.id).assertSuccess
                state.products remove singleRemove
                state.removedProducts add singleRemove.id

                //batch delete random multiple 50 products
                val batchProductsToRemove = randomCreatedProducts.takeRight(50)
                val batchRemove = batchProductsToRemove.map(_.id)
                db.batchRemove(batchRemove).assertSuccess
                batchProductsToRemove foreach {
                  product =>
                    state.products remove product
                    state.removedProducts add product.id
                }
                Actor.same

              case AssertState(removeAsserted) =>
                logger.info(s"UserId: $userId - AssertState. Asserting User.")
                //assert the state of the User itself. This is a static record and does not mutate.
                db.get(state.user.id).assertSuccess should contain(state.user)
                val shuffledCreatedProducts = Random.shuffle(state.products)
                logger.info(s"UserId: $userId - start asserting ${shuffledCreatedProducts.size} createdProducts. removeAsserted = $removeAsserted.")
                //assert the state of created products in the User's state and remove products from state if required.
                val removedProducts =
                  shuffledCreatedProducts.foldLeft(0) {
                    case (removeCount, product) =>
                      Try(db.get(product.id).assertSuccess should contain(product)) recoverWith {
                        case ex =>
                          ex.printStackTrace()
                          System.exit(0)
                          throw ex
                      }
                      removeAsserted match {
                        case Remove(maxToRemove) if removeCount < maxToRemove =>
                          state.products remove product
                          removeCount + 1
                        case RemoveAll =>
                          state.products remove product
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
                          ex.printStackTrace()
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

      print("How many minutes to run the test for (hit Enter for 10 minutes): ")
      val runFor = Try(readInt().minutes) getOrElse 10.minutes

      //Create actorSystem's root actor
      val guardian =
        Actor.immutable[TestCommand] {
          case (ctx, command) =>
            command match {
              case StartTest => //starts the test
                ids.set(maxUsers + 1) //keep reserved ids for User accounts.
                (1 to maxUsers) map { //create Users in the database
                  id =>
                    val user = User(id)
                    db.put(user.id, user).assertSuccess
                    user
                } foreach {
                  user =>
                    //spawn User simulator Actor for the created Users.
                    ctx.spawn(
                      userSimulatorActor(
                        UserState(
                          user = user,
                          products = mutable.SortedSet()(Ordering.by[Product, Int](_.id)),
                          removedProducts = mutable.Set(),
                          productsCreatedCountBeforeAssertion = 0)
                      ),
                      name = user.id.toString
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
               |*****************************************************************
                  """.stripMargin
          )
          System.exit(0) //terminate test when a response is received.
          assertion
      }
    }
  }
}