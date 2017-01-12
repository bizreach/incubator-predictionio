/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.predictionio.data.storage.elasticsearch

import org.apache.http.entity.StringEntity
import org.json4s.{JValue, Formats}
import org.json4s.native.Serialization.read
import org.json4s.native.JsonMethods._
import scala.collection.mutable.ArrayBuffer
import org.elasticsearch.client.RestClient
import collection.JavaConversions._

object ESUtils {
  val scrollLife = "60000"
  def getAll[T : Manifest](
    client: RestClient,
    index: String,
    estype: String,
    query: String)(
    implicit formats: Formats): Seq[T] = {

    val response = client.performRequest(
      "POST",
      s"/$index/$estype/search",
      Map("scroll" -> "1m"),
      new StringEntity(query))
    val responseJValue = parse(response.getEntity.toString)
    val scrollId = (responseJValue \ "_scroll_id").extract[String]
    val scrollBody = new StringEntity(("scroll" -> "1m", "scroll_id" -> scrollId).toString)

    def scroll(hits: Seq[JValue], results: Seq[T]): Seq[T] = {
      if (hits.isEmpty) results
      else {
        val response = client.performRequest(
          "POST",
          "/search/scroll",
          Map[String, String](),
          scrollBody)
        val responseJValue = parse(response.getEntity.toString)
        scroll(
          (responseJValue \ "hits" \ "hits").extract[Seq[JValue]],
          hits.map(h => read[T](h.toString)) ++ results
        )
      }
    }

    scroll((responseJValue \ "hits" \ "hits").extract[Seq[JValue]], Nil)
  }
}
