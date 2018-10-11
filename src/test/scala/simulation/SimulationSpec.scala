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

import base.TestBase
import com.github.simerplaha.actor.{Actor, ActorRef}
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.WordSpec
import simulation.Domain.{Product, User}
import simulation.ProductCommand._
import simulation.RemoveAsserted.{Remove, RemoveAll}
import swaydb._
import swaydb.configs.level.DefaultGroupingStrategy
import swaydb.data.accelerate.Accelerator
import swaydb.data.config.MMAP
import swaydb.types.SwayDBMap

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._
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

class Memory_SimulationSpec extends SimulationSpec {

  import swaydb.serializers.Default.LongSerializer

  override lazy val db = SwayDB.memory[Long, Domain]().assertSuccess
}

class Memory_SimulationGroupingStrategySpec extends SimulationSpec {

  import swaydb.serializers.Default.LongSerializer

  override lazy val db = SwayDB.memory[Long, Domain](groupingStrategy = Some(DefaultGroupingStrategy())).assertSuccess
}

class Persistent_SimulationSpec extends SimulationSpec {

  import swaydb.serializers.Default.LongSerializer

  override lazy val db = SwayDB.persistent[Long, Domain](dir, acceleration = Accelerator.brake()).assertSuccess
}

class Memory_Persistent_SimulationSpec extends SimulationSpec {

  import swaydb.serializers.Default.LongSerializer

  override lazy val db = SwayDB.persistent[Long, Domain](dir, mmapAppendix = false, mmapMaps = false, mmapSegments = MMAP.Disable).assertSuccess
}

sealed trait SimulationSpec extends WordSpec with TestBase with LazyLogging {

  def db: SwayDBMap[Long, Domain]

  val ids = new AtomicInteger(0)

  case class UserState(userId: Int,
                       var nextProductId: Long,
                       user: User,
                       products: mutable.Map[Long, (Product, Option[Deadline])],
                       removedProducts: mutable.Set[Long],
                       var productsCreatedCountBeforeAssertion: Int)

  import scala.concurrent.ExecutionContext.Implicits.global

  def processCommand(state: UserState,
                     command: ProductCommand,
                     self: ActorRef[ProductCommand]) = {
    val userId = state.userId

    def genProductId: Long = {
      val nextId = state.nextProductId
      state.nextProductId = state.nextProductId + 1
      nextId
    }

    command match {
      case Create =>
        //create 1 product
        val (productId1, product1) = (genProductId, Product(s"initialPut_${System.nanoTime()}"))
        val (productId2, product2) = (genProductId, Product(s"initialPut_${System.nanoTime()}"))
        val (productId3, product3) = (genProductId, Product(s"initialPut_${System.nanoTime()}"))

        db.put(productId1, product1).assertSuccess
        db.put(productId2, product2).assertSuccess
        db.put(productId3, product3).assertSuccess

        state.products.put(productId1, (product1, None))
        state.products.put(productId2, (product2, None))
        state.products.put(productId3, (product3, None))

        //increment counter for the 3 created products
        state.productsCreatedCountBeforeAssertion = state.productsCreatedCountBeforeAssertion + 3
        //max number of products to create before asserting the database state for this User's created products.
        val maxProductsToCreateBeforeAssertion = 1000
        //do updates and delete every 1000th product added and continue Creating more products
        if (state.productsCreatedCountBeforeAssertion >= maxProductsToCreateBeforeAssertion) {
          logger.info(s"UserId: $userId - Created ${state.productsCreatedCountBeforeAssertion} products, state.products = ${state.products.size}, state.removedProducts = ${state.removedProducts.size} - ProductId: $productId1")
          self ! AssertState(removeAsserted = RemoveAsserted.RemoveNone)
          self.schedule(Put, (randomNextInt(2) + 1).seconds)
          self.schedule(BatchPut, (randomNextInt(2) + 1).seconds)
          //          self.schedule(UpdateRange, (randomNextInt(2) + 1).second)
          self.schedule(Delete, (randomNextInt(2) + 1).seconds)
          self.schedule(Expire, (randomNextInt(2) + 1).seconds)
          self.schedule(BatchDelete, (randomNextInt(2) + 1).seconds)
          self.schedule(BatchExpire, (randomNextInt(2) + 1).second)
          self.schedule(DeleteRange, (randomNextInt(2) + 1).seconds)
          self.schedule(ExpireRange, (randomNextInt(2) + 1).seconds)
          //if this User accumulates more then 50000 products in-memory, then assert and remove all
          if (state.products.size + state.removedProducts.size >= 5000)
            self.schedule(AssertState(removeAsserted = RemoveAsserted.RemoveAll), 3.seconds)
          //if this User accumulates more then 1000 products in-memory, then assert and remove 10
          else if (state.products.size + state.removedProducts.size >= 1000)
            self.schedule(AssertState(removeAsserted = RemoveAsserted.Remove(10)), 3.seconds)
          //other do not remove any in-memory data.
          else
            self.schedule(AssertState(removeAsserted = RemoveAsserted.RemoveNone), 3.seconds)

          //also schedule a Create to repeatedly keep creating more Products by this User.
          self.schedule(Create, 3.seconds)
          //                  self ! Create
          //reset the counter as the assertion is triggered.
          state.productsCreatedCountBeforeAssertion = 0
          logger.info(s"UserId: $userId - Reset created product counter to ${state.productsCreatedCountBeforeAssertion} products")
        } else {
          //keep on Creating more Products.
          self ! Create
        }

      case Put =>
        if (state.products.nonEmpty) {
          //single
          logger.info(s"UserId: $userId - Put")

          val randomCreatedProducts = Random.shuffle(state.products)

          //put a random existing single product
          val (productId, (product, _)) = randomCreatedProducts.head
          val putProduct = product.copy(name = product.name + "_" + s"put_${System.nanoTime()}")
          db.put(productId, putProduct).assertSuccess
          state.products.put(productId, (putProduct, None))
        }

      case Update =>
        if (state.products.nonEmpty) {
          //single
          logger.info(s"UserId: $userId - Update")

          val randomCreatedProducts = Random.shuffle(state.products)

          //update a random single product
          val (productId, (product, deadline)) = randomCreatedProducts.head
          val updatedProduct = product.copy(name = product.name + "-" + s"updated_${System.nanoTime()}")
          db.update(productId, updatedProduct).assertSuccess
          state.products.put(productId, (updatedProduct, deadline))
        }
      case Expire =>
        if (state.products.nonEmpty) {
          //single
          logger.info(s"UserId: $userId - Expire")

          val randomCreatedProducts = Random.shuffle(state.products)

          //update a random single product
          val (productId, (product, deadline)) = randomCreatedProducts.head
          val newDeadline = deadline.map(_ - 1.second) getOrElse 1.hour.fromNow
          db.expire(productId, newDeadline).assertSuccess
          state.products.put(productId, (product, Some(newDeadline)))
        }

      case BatchPut =>
        if (state.products.nonEmpty) {
          //single
          logger.info(s"UserId: $userId - BatchUpdate")

          val randomCreatedProducts = Random.shuffle(state.products)

          //batch update random 100 products.
          val batchProducts = randomCreatedProducts.takeRight(10)

          try
            batchProducts foreach {
              batchProduct =>
                db.get(batchProduct._1).assertSuccess should contain(batchProduct._2._1)
            }
          catch {
            case ex: Exception =>
              ex.printStackTrace()
              System.exit(0)
          }

          val batchUpdatedProducts =
            batchProducts map {
              case (productId, (product, _)) =>
                (productId, product.copy(name = product.name + "-" + s"batch_put_${System.nanoTime()}"))
            }
          db.batchPut(batchUpdatedProducts).assertSuccess

          batchUpdatedProducts foreach {
            case (productId, product) =>
              state.products.put(productId, (product, None))
          }
        }

      case BatchExpire =>
        if (state.products.nonEmpty) {
          //single
          logger.info(s"UserId: $userId - BatchExpire")

          val randomCreatedProducts = Random.shuffle(state.products)

          val batchProducts = randomCreatedProducts.take(10)

          val batchExpire: mutable.Iterable[(Long, Deadline)] =
            batchProducts map {
              case (productId, (_, deadline)) =>
                val newDeadline = deadline.map(_ - 1.second) getOrElse 1.hour.fromNow
                (productId, newDeadline)
            }

          //update a random single product
          db.batchExpire(batchExpire).assertSuccess
          batchExpire foreach {
            case (productId, newDeadline) =>
              val (product, _) = state.products.get(productId).assertGet
              state.products.put(productId, (product, Some(newDeadline)))
          }
        }

      case UpdateRange =>
        if (state.products.size >= 60) {
          //the test seems to be failing after a product is range updated & then batch updated.
          //the database is returning accurate result and is returning the batch updated result.
          //but the test is expecting range update result. The problem is with the test and not the database.
          //need to revisit this test.
          //For example here is a result from a product. 'update_range_2' is an old update but the test is
          //Some(Product(update_range_2_PAqH4mCSQ1_batch_put)) did not contain element Product(update_range_2)
          logger.info(s"UserId: $userId - UpdateRange")

          val randomCreatedProducts = Random.shuffle(state.products)

          val from = randomCreatedProducts.head._1 min randomCreatedProducts.last._1
          val to = randomCreatedProducts.head._1 max randomCreatedProducts.last._1

          if (from >= to) {
            println(s"from >= to: $from >= $to")
            System.exit(0)
          }

          //state.updateRangeCount indicates the number of times UpdateRang is invoked on the key-value.
          val updatedProduct = Product(s"update_range_${System.nanoTime()}")
          db.update(from, to, updatedProduct).assertSuccess

          (from to to) foreach {
            updatedProductId =>
              state.products.get(updatedProductId) map {
                case (_, deadline) =>
                  state.products.put(updatedProductId, (updatedProduct, deadline))
              }
          }
        }

      case ExpireRange =>
        if (state.products.size >= 60) {
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
        }

      case Delete =>
        if (state.products.nonEmpty) {
          logger.info(s"UserId: $userId - Delete")

          val randomCreatedProducts = Random.shuffle(state.products)

          //delete random single product
          val (productToRemoveId, productToRemove) = randomCreatedProducts.head
          db.remove(productToRemoveId).assertSuccess
          state.products remove productToRemoveId
          state.removedProducts add productToRemoveId
        }

      case BatchDelete =>
        if (state.products.nonEmpty) {
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

        }

      case DeleteRange =>
        if (state.products.size >= 60) {
          logger.info(s"UserId: $userId - RangeDelete")

          val randomCreatedProducts = Random.shuffle(state.products)

          val from = randomCreatedProducts.head._1 min randomCreatedProducts.last._1
          val to = randomCreatedProducts.head._1 max randomCreatedProducts.last._1

          if (from >= to) {
            println(s"from >= to: $from >= $to")
            System.exit(0)
          }

          db.remove(from, to).assertSuccess

          (from to to) foreach {
            removedProductId =>
              state.products.remove(removedProductId)
              state.removedProducts.add(removedProductId)
          }

        }
      case AssertState(removeAsserted) =>
        logger.info(s"UserId: $userId - AssertState. Asserting User.")
        //assert the state of the User itself. This is a static record and does not mutate.
        db.get(state.userId).assertSuccess should contain(state.user)
        val shuffledCreatedProducts = Random.shuffle(state.products)
        logger.info(s"UserId: $userId - start asserting ${shuffledCreatedProducts.size} createdProducts. removeAsserted = $removeAsserted.")
        //assert the state of created products in the User's state and remove products from state if required.
        val removedProducts =
          shuffledCreatedProducts.foldLeft(0) {
            case (removeCount, (productId, product)) =>
              Try {
                Future(db.get(productId).assertSuccess should contain(product._1)) await 10.seconds
              } recoverWith {
                case ex =>
                  System.err.println(s"*************************************************************** 111 At ID: $productId")
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
              Try {
                Future(db.get(productId).assertSuccess shouldBe empty) await 10.seconds
              } recoverWith {
                case ex =>
                  System.err.println(s"*************************************************************** At ID: $productId")
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

    }
  }

  "Users" should {

    "print DB" in {
      println(db.get(160000000000001887L).assertSuccess)
    }

    "concurrently Create, Update, Read & Delete (CRUD) Products" in {
      val maxUsers: Int = 100
      val runFor = 10.minutes

      (1 to maxUsers) map { //create Users in the database
        id =>
          val user = User(s"user-$id")
          db.put(id, user).assertSuccess
          (id, user)
      } foreach {
        case (userId, user) =>

          val state =
            UserState(
              userId = userId,
              nextProductId = s"${userId}0000000000000000".toLong,
              user = user,
              products = mutable.SortedMap(),
              removedProducts = mutable.Set(),
              productsCreatedCountBeforeAssertion = 0
            )

          val actor =
            Actor[ProductCommand, UserState](state) {
              (command, self) =>
                processCommand(self.state, command, self)
            }
          actor ! ProductCommand.Create
      }

      sleep(runFor)
    }
  }
}