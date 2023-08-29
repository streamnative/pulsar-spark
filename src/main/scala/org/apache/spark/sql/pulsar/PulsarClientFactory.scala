/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.pulsar

import java.{util => ju}

import org.apache.pulsar.client.impl.PulsarClientImpl

import org.apache.spark.SparkConf
import org.apache.spark.util.Utils

trait PulsarClientFactory {
  def getOrCreate(params: ju.Map[String, Object]): PulsarClientImpl
}

class DefaultPulsarClientFactory extends PulsarClientFactory {
  def getOrCreate(params: ju.Map[String, Object]): PulsarClientImpl = {
    CachedPulsarClient.getOrCreate(params)
  }
}

object PulsarClientFactory {
  val PulsarClientFactoryClassOption = "org.apache.spark.sql.pulsar.PulsarClientFactoryClass"
  def getOrCreate(sparkConf: SparkConf, params: ju.Map[String, Object]): PulsarClientImpl = {
    getFactory(sparkConf).getOrCreate(params)
  }

  private def getFactory(sparkConf: SparkConf): PulsarClientFactory = {
    sparkConf.getOption(PulsarClientFactoryClassOption) match {
      case Some(factoryClassName) =>
        Utils.classForName(factoryClassName).getConstructor()
          .newInstance().asInstanceOf[PulsarClientFactory]
      case None => new DefaultPulsarClientFactory()
    }
  }
}


