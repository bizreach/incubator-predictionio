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

import java.io.IOException

import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.apache.http.entity.ContentType
import org.apache.http.nio.entity.NStringEntity
import org.apache.http.util.EntityUtils
import org.apache.predictionio.data.storage.App
import org.apache.predictionio.data.storage.Apps
import org.apache.predictionio.data.storage.StorageClientConfig
import org.elasticsearch.client.RestClient
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization.write

import grizzled.slf4j.Logging
import org.elasticsearch.client.ResponseException

/** Elasticsearch implementation of Items. */
class ESApps(client: RestClient, config: StorageClientConfig, index: String)
    extends Apps with Logging {
  implicit val formats = DefaultFormats.lossless
  private val estype = "apps"
  private val seq = new ESSequences(client, config, index)

  ESUtils.createIndex(client, index)
  val mappingJson =
    (estype ->
      ("_all" -> ("enabled" -> 0))~
      ("properties" ->
        ("id" -> ("type" -> "keyword")) ~
        ("name" -> ("type" -> "keyword"))))
  ESUtils.createMapping(client, index, estype, compact(render(mappingJson)))

  def insert(app: App): Option[Int] = {
    val id =
      if (app.id == 0) {
        var roll = seq.genNext(estype)
        while (!get(roll).isEmpty) roll = seq.genNext(estype)
        roll
      } else app.id
    update(app.copy(id = id))
    Some(id)
  }

  def get(id: Int): Option[App] = {
    try {
      val response = client.performRequest(
        "GET",
        s"/$index/$estype/$id",
        Map.empty[String, String].asJava)
      val jsonResponse = parse(EntityUtils.toString(response.getEntity))
      (jsonResponse \ "found").extract[Boolean] match {
        case true =>
          Some((jsonResponse \ "_source").extract[App])
        case _ =>
          None
      }
    } catch {
      case e: ResponseException =>
        e.getResponse.getStatusLine.getStatusCode match {
          case 404 => None
          case _ =>
            error(s"Failed to access to /$index/$estype/$id", e)
            None
        }
      case e: IOException =>
        error(s"Failed to access to /$index/$estype/$id", e)
        None
    }
  }

  def getByName(name: String): Option[App] = {
    try {
      val json =
        ("query" ->
          ("term" ->
            ("name" -> name)))
      val entity = new NStringEntity(compact(render(json)), ContentType.APPLICATION_JSON)
      val response = client.performRequest(
        "POST",
        s"/$index/$estype/_search",
        Map.empty[String, String].asJava,
        entity)
      val jsonResponse = parse(EntityUtils.toString(response.getEntity))
      (jsonResponse \ "hits" \ "total").extract[Long] match {
        case 0 => None
        case _ =>
          val results = (jsonResponse \ "hits" \ "hits").extract[Seq[JValue]]
          val result = (results.head \ "_source").extract[App]
          Some(result)
      }
    } catch {
      case e: IOException =>
        error(s"Failed to access to /$index/$estype/_search", e)
        None
    }
  }

  def getAll(): Seq[App] = {
    try {
      val json =
        ("query" ->
          ("match_all" -> List.empty))
      ESUtils.getAll[App](client, index, estype, compact(render(json)))
    } catch {
      case e: IOException =>
        error("Failed to access to /$index/$estype/_search", e)
        Nil
    }
  }

  def update(app: App): Unit = {
    val id = app.id.toString
    try {
      val entity = new NStringEntity(write(app), ContentType.APPLICATION_JSON);
      val response = client.performRequest(
        "POST",
        s"/$index/$estype/$id",
        Map.empty[String, String].asJava,
        entity)
      val jsonResponse = parse(EntityUtils.toString(response.getEntity))
      val result = (jsonResponse \ "result").extract[String]
      result match {
        case "created" =>
        case "updated" =>
        case _ =>
          error(s"[$result] Failed to update $index/$estype/$id")
      }
    } catch {
      case e: IOException =>
        error(s"Failed to update $index/$estype/$id", e)
    }
  }

  def delete(id: Int): Unit = {
    try {
      val response = client.performRequest(
        "DELETE",
        s"/$index/$estype/$id",
        Map.empty[String, String].asJava)
      val json = parse(EntityUtils.toString(response.getEntity))
      val result = (json \ "result").extract[String]
      result match {
        case "deleted" =>
        case _ =>
          error(s"[$result] Failed to update $index/$estype/$id")
      }
    } catch {
      case e: IOException =>
        error(s"Failed to update $index/$estype/id", e)
    }
  }
}
