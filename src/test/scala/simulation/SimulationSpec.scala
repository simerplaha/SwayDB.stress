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
import akka.typed.{ActorSystem, Behavior}
import base.{CommonAssertions, TestBase}
import simulation.Domain.{Product, User}
import simulation.ProductCommand._
import swaydb._
import swaydb.data.accelerate.Accelerator
import swaydb.data.config.MMAP

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Random

sealed trait ProductCommand
object ProductCommand {
  case object Create extends ProductCommand
  case object Update extends ProductCommand
  case object Delete extends ProductCommand
  case object AssertState extends ProductCommand
}

class SimulationSpec extends TestBase with CommonAssertions {

  import swaydb.serializers.Default.IntSerializer

  val db = SwayDB.memory[Int, Domain]().assertSuccess
  //    val db = SwayDB.persistent[Int, Domain](dir, acceleration = Accelerator.brake()).assertSuccess
  //  val db = SwayDB.persistent[Int, Domain](dir, mmapAppendix = false, mmapMaps = false, mmapSegments = MMAP.Disable).assertSuccess

  val ids = new AtomicInteger(0)

  def genId = ids.incrementAndGet()

  //  override def deleteDB: Boolean = false

  case class UserState(user: User,
                       createdProducts: mutable.Set[Product],
                       removedProducts: mutable.Set[Int])

  def userSimulatorActor(state: UserState): Behavior[ProductCommand] = {
    Actor.deferred[ProductCommand] {
      ctx =>
        println(s"${state.user.id}: UserActor started")
        ctx.self ! Create

        Actor.immutable[ProductCommand] {
          case (ctx, message) =>
            val userId = state.user.id
            val self = ctx.self
            //        println(s"Processing message: $message")
            message match {

              case Create =>
                //create 1 product
                val product = Product(genId, randomCharacters() + "_" + 1)
                db.put(product.id, product).assertSuccess
                state.createdProducts.add(product)

                //batch Create 2 products
                val batchProduct1 = Product(genId, randomCharacters() + "_" + 1)
                val batchProduct2 = Product(genId, randomCharacters() + "_" + 1)
                db.batchPut(Seq((batchProduct1.id, batchProduct1), (batchProduct2.id, batchProduct2))).assertSuccess
                state.createdProducts.add(batchProduct1)
                state.createdProducts.add(batchProduct2)

                //do updates and delete every 1000th product added and continue Creating more products
                if (state.createdProducts.size % 5000 == 0) {
                  println(s"UserId: $userId - Created 5000 products: ${product.id}")
                  self ! AssertState
                  self ! Update
                  self ! Delete
                  self ! AssertState
                }

                self ! Create
                Actor.same

              case Update if state.createdProducts.nonEmpty =>
                //single
                println(s"UserId: $userId - Update")
                val product = state.createdProducts.head
                val updatedProduct = product.copy(name = randomCharacters())
                db.put(product.id, updatedProduct).assertSuccess
                state.createdProducts remove product
                state.createdProducts add updatedProduct

                //batch
                val batchProduct = state.createdProducts.takeRight(10)
                val batchUpdatedProducts = batchProduct.map(product => (product.id, product.copy(name = randomCharacters())))
                db.batchPut(batchUpdatedProducts).assertSuccess
                batchProduct.foreach(state.createdProducts.remove)
                batchUpdatedProducts.foreach { case (_, product) => state.createdProducts.add(product) }
                Actor.same

              case Delete if state.createdProducts.nonEmpty =>
                println(s"UserId: $userId - Delete")
                //single delete
                val singleRemove = state.createdProducts.head
                db.remove(singleRemove.id).assertSuccess
                state.createdProducts remove singleRemove
                state.removedProducts add singleRemove.id

                //batch delete
                val batchProductsToRemove = state.createdProducts.takeRight(10)
                val batchRemove = batchProductsToRemove.map(_.id)
                db.batchRemove(batchRemove).assertSuccess
                batchProductsToRemove.foreach {
                  product =>
                    state.createdProducts remove product
                    state.removedProducts add product.id
                }
                Actor.same

              case AssertState =>
                println(s"UserId: $userId - AssertState")
                db.get(state.user.id).assertSuccess should contain(state.user)
                //assert created products
                Random.shuffle(state.createdProducts).take(1000).foreach {
                  product =>
                    if (product.id % 500 == 0) println(s"UserId: $userId: Asserting state on 500th created product id: ${product.id}")
                    db.get(product.id).assertSuccess should contain(product)
                    state.createdProducts remove product
                }
                ///assert removed products do not exists
                Random.shuffle(state.removedProducts).take(1000).foreach {
                  productId =>
                    if (productId % 500 == 0) println(s"UserId: $userId: Asserting state on 500th removed product id: $productId")
                    db.get(productId).assertSuccess shouldBe empty
                    state.removedProducts remove productId
                }

                Actor.same
              case _ =>

                Actor.same
            }
        }
    }
  }

  "Users" should {
    "concurrently Create, Update, Read & Delete (CRUD) Products" in {
      val guardian =
        Actor.immutable[User] {
          case (ctx, user) =>
            //create the user
            db.put(user.id, user).assertSuccess
            //spawn User simulator actor
            ctx.spawn(userSimulatorActor(UserState(user, mutable.Set(), mutable.Set())), user.id.toString)
            Actor.same
        }

      val system: ActorSystem[User] = ActorSystem(guardian, "simulation-test")

      (1 to 10) foreach {
        _ =>
          system ! User(genId)
      }
      //Sleep for how ever long this test should run.
      //this test keeps expected database state in-memory for validation/assertion which will keep growing as
      //the test is running. A larger heap space is required if this test is kept running for longer period.
      sleep(30.seconds)
    }
  }

}
