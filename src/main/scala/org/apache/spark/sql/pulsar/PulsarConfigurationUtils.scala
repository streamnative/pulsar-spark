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

import java.util.Locale

import reflect.runtime.universe._

import org.apache.pulsar.client.impl.conf.{
  ClientConfigurationData,
  ConsumerConfigurationData,
  ProducerConfigurationData,
  ReaderConfigurationData
}
import org.apache.pulsar.shade.com.fasterxml.jackson.annotation.JsonIgnore

object PulsarConfigurationUtils {

  private def nonIgnoredFields[T: TypeTag] = {
    // a field is a Term that is a Var or a Val
    val fields =
      typeOf[T].members.collect { case s: TermSymbol => s }.filter(s => s.isVal || s.isVar)

    // then only keep the ones without a JsonIgnore annotation
    val ignores = fields
      .flatMap(f => f.annotations.find(_.tree.tpe =:= typeOf[JsonIgnore]).map((f, _)))
      .map(t => t._1)
      .toList

    fields.filterNot(ignores.contains).map(_.name.toString)
  }

  private def insensitive2Sensitive[T: TypeTag]: Map[String, String] = {
    nonIgnoredFields[T].map(s => s.toLowerCase(Locale.ROOT) -> s).toMap
  }

  val clientConfKeys = insensitive2Sensitive[ClientConfigurationData]
  val producerConfKeys = insensitive2Sensitive[ProducerConfigurationData]
  val consumerConfKeys = insensitive2Sensitive[ConsumerConfigurationData[_]]
  val readerConfKeys = insensitive2Sensitive[ReaderConfigurationData[_]]
}
