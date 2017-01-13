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
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import org.apache.http.entity.ContentType
import org.apache.http.nio.entity.NStringEntity
import org.apache.http.util.EntityUtils
import org.apache.predictionio.data.storage.Event
import org.apache.predictionio.data.storage.LEvents
import org.apache.predictionio.data.storage.StorageClientConfig
import org.elasticsearch.client.RestClient
import org.joda.time.DateTime
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization.write
import org.json4s.ext.JodaTimeSerializers

import grizzled.slf4j.Logging
import org.elasticsearch.client.ResponseException

class ESLEvents(val client: RestClient, config: StorageClientConfig, val index: String)
    extends LEvents with Logging {
  implicit val formats = DefaultFormats.lossless ++ JodaTimeSerializers.all
  private val seq = new ESSequences(client, config, index)
  private val seqName = "events"

  def getEsType(appId: Int, channelId: Option[Int] = None): String = {
    channelId.map { ch =>
      s"${appId}_${ch}"
    }.getOrElse {
      s"${appId}"
    }
  }

  override def init(appId: Int, channelId: Option[Int] = None): Boolean = {
    val estype = getEsType(appId, channelId)
    ESUtils.createIndex(client, index)
    val json =
      (estype ->
        ("_all" -> ("enabled" -> 0)) ~
        ("properties" ->
          ("name" -> ("type" -> "keyword")) ~
          ("eventId" -> ("type" -> "keyword")) ~
          ("event" -> ("type" -> "keyword")) ~
          ("entityType" -> ("type" -> "keyword")) ~
          ("entityId" -> ("type" -> "keyword")) ~
          ("targetEntityType" -> ("type" -> "keyword")) ~
          ("targetEntityId" -> ("type" -> "keyword")) ~
          ("properties" ->
            ("type" -> "nested") ~
            ("properties" ->
              ("fields" -> ("type" -> "nested") ~
                ("properties" ->
                  ("user" -> ("type" -> "long")) ~
                  ("num" -> ("type" -> "long")))))) ~
                  ("eventTime" -> ("type" -> "date")) ~
                  ("tags" -> ("type" -> "keyword")) ~
                  ("prId" -> ("type" -> "keyword")) ~
                  ("creationTime" -> ("type" -> "date"))))
    ESUtils.createMapping(client, index, estype, compact(render(json)))
    true
  }

  override def remove(appId: Int, channelId: Option[Int] = None): Boolean = {
    val estype = getEsType(appId, channelId)
    try {
      val json =
        ("query" ->
          ("match_all" -> List.empty))
      val entity = new NStringEntity(compact(render(json)), ContentType.APPLICATION_JSON)
      client.performRequest(
        "POST",
        s"/$index/$estype/_delete_by_query",
        Map.empty[String, String].asJava,
        entity).getStatusLine.getStatusCode match {
          case 200 => true
          case _ =>
            error(s"Failed to remove $index/$estype")
            false
        }
    } catch {
      case e: Exception =>
        error(s"Fail to remove $index/$estype", e)
        false
    }
  }

  override def close(): Unit = {
    // nothing to do
  }

  override def futureInsert(
    event: Event, appId: Int, channelId: Option[Int])(implicit ec: ExecutionContext): Future[String] = {
    // TODO: Future[Either]で返すようにする
    Future {
      val estype = getEsType(appId, channelId)
      val id = event.eventId.getOrElse {
        var roll = seq.genNext(seqName)
        while (exists(estype, roll)) roll = seq.genNext(seqName)
        roll.toString
      }
      val json = write(event.copy(eventId = Some(id)))
      try {
        val entity = new NStringEntity(json, ContentType.APPLICATION_JSON);
        val response = client.performRequest(
          "POST",
          s"/$index/$estype/$id",
          Map.empty[String, String].asJava,
          entity)
        val jsonResponse = parse(EntityUtils.toString(response.getEntity))
        val result = (jsonResponse \ "result").extract[String]
        result match {
          case "created" => id
          case "updated" => id
          case _ =>
            error(s"[$result] Failed to update $index/$estype/$id")
            ""
        }
      } catch {
        case e: IOException =>
          error(s"Failed to update $index/$estype/$id: $json", e)
          ""
      }
    }
  }

  private def exists(estype: String, id: Int): Boolean = {
    try {
      client.performRequest(
        "GET",
        s"/$index/$estype/$id",
        Map.empty[String, String].asJava).getStatusLine.getStatusCode match {
          case 200 => true
          case _ => false
        }
    } catch {
      case e: ResponseException =>
        e.getResponse.getStatusLine.getStatusCode match {
          case 404 => false
          case _ =>
            error(s"Failed to access to /$index/$estype/$id", e)
            false
        }
      case e: IOException =>
        error(s"Failed to access to $index/$estype/$id", e)
        false
    }
  }

  override def futureGet(
    eventId: String, appId: Int, channelId: Option[Int])(implicit ec: ExecutionContext): Future[Option[Event]] = {
    // TODO: Future[Either]で返すようにする
    Future {
      val estype = getEsType(appId, channelId)
      try {
        val json =
          ("query" ->
            ("term" ->
              ("eventId" -> eventId)))
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
            val result = (results.head \ "_source").extract[Event]
            Some(result)
        }
      } catch {
        case e: IOException =>
          error("Failed to access to /$index/$estype/_search", e)
          None
      }
    }
  }

  override def futureDelete(
    eventId: String,
    appId: Int,
    channelId: Option[Int])(implicit ec: ExecutionContext): Future[Boolean] = {
    Future {
      val estype = getEsType(appId, channelId)
      try {
        val json =
          ("query" ->
            ("term" ->
              ("eventId" -> eventId)))
        val entity = new NStringEntity(compact(render(json)), ContentType.APPLICATION_JSON)
        val response = client.performRequest(
          "POST",
          s"/$index/$estype/_delete_by_query",
          Map.empty[String, String].asJava)
        val jsonResponse = parse(EntityUtils.toString(response.getEntity))
        val result = (jsonResponse \ "result").extract[String]
        result match {
          case "deleted" => true
          case _ =>
            error(s"[$result] Failed to update $index/$estype:$eventId")
            false
        }
      } catch {
        case e: IOException =>
          error(s"Failed to update $index/$estype:$eventId", e)
          false
      }
    }
  }

  override def futureFind(
    appId: Int,
    channelId: Option[Int] = None,
    startTime: Option[DateTime] = None,
    untilTime: Option[DateTime] = None,
    entityType: Option[String] = None,
    entityId: Option[String] = None,
    eventNames: Option[Seq[String]] = None,
    targetEntityType: Option[Option[String]] = None,
    targetEntityId: Option[Option[String]] = None,
    limit: Option[Int] = None,
    reversed: Option[Boolean] = None)(implicit ec: ExecutionContext): Future[Iterator[Event]] = {
    // TODO: FutureMapとか使うべきなのでは。。。
    Future {
      val estype = getEsType(appId, channelId)
      try {
        val query = ESUtils.createEventQuery(startTime, untilTime, entityType, entityId, eventNames, targetEntityType, targetEntityId, None)
        ESUtils.getAll[Event](client, index, estype, query).toIterator
      } catch {
        case e: IOException =>
          error(e.getMessage)
          Iterator[Event]()
      }
    }
  }

}
