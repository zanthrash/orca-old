/*
 * Copyright 2014 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.spinnaker.orca.pipeline.persistence.jedis

import com.netflix.spinnaker.kork.jedis.EmbeddedRedis
import com.netflix.spinnaker.orca.jackson.OrcaObjectMapper
import com.netflix.spinnaker.orca.pipeline.persistence.PipelineStoreTck
import spock.lang.AutoCleanup
import spock.lang.Shared

class JedisPipelineStoreSpec extends PipelineStoreTck<JedisPipelineStore> {

  @Shared @AutoCleanup("destroy") EmbeddedRedis embeddedRedis
  @Shared mapper = new OrcaObjectMapper()

  def setupSpec() {
    embeddedRedis = EmbeddedRedis.embed()
  }

  def cleanup() {
    embeddedRedis.jedis.flushDB()
  }

  def jedis = embeddedRedis.jedisCommands

  @Override
  JedisPipelineStore createPipelineStore() {
    new JedisPipelineStore(jedis, mapper)
  }
}
