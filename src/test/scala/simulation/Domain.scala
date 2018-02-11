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

import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf
import swaydb.serializers.Serializer

sealed trait Domain {
  val id: Int
}

object Domain {
  case class User(id: Int) extends Domain
  case class Product(id: Int, name: String) extends Domain

  implicit object DomainSerializer extends Serializer[Domain] {
    override def write(data: Domain): Slice[Byte] =
      data match {
        case User(id) =>
          Slice
            .create(ByteSizeOf.int * 2)
            .addInt(1)
            .addInt(id)

        case Product(id, name) =>
          Slice
            .create(100)
            .addInt(2)
            .addInt(id)
            .addString(name)
            .close()
      }

    override def read(data: Slice[Byte]): Domain = {
      val reader = data.createReader()
      val dataId = reader.readInt()
      val result =
        if (dataId == 1) {
          val userId = reader.readInt()
          User(userId)
        } else {
          val productId = reader.readInt()
          val productName = reader.readString()
          Product(productId, productName)
        }
      result
    }
  }
}
