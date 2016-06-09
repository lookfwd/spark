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

package org.apache.spark.streaming.rabbitmq

import java.util.{List => JList, Map => JMap, Set => JSet}
import org.apache.spark.streaming.api.java._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.InputDStream

import org.apache.spark.streaming.rabbitmq._
import org.apache.spark.streaming.rabbitmq.distributed.RabbitMQDistributedKey
import org.apache.spark.streaming.rabbitmq.models.ExchangeAndRouting


private[rabbitmq] class RabbitMQUtilsPythonHelper {

  def createStream(
      jssc: JavaStreamingContext,
      host: String,
      exchange: String,
      routing_key: String,
      queue: String,
      storageLevel: StorageLevel
      ): InputDStream[String] = {


    jssc.ssc.withNamedScope("rabbitmq stream") {
      val rabbitMQParams = Map.empty[String, String]

      val rabbitMQConnection1 = Map(
        "hosts" -> host,
        "queueName" -> queue,
        "exchangeName" -> exchange
      )

      val distributedKey = Seq(
        RabbitMQDistributedKey(
          routing_key,
          new ExchangeAndRouting(exchange, queue),
          rabbitMQConnection1
        )
      )

      RabbitMQUtils.createDistributedStream[String](jssc.ssc, distributedKey, rabbitMQParams)
    }
  }
}
