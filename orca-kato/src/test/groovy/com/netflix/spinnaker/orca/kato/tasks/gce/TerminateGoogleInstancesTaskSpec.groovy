/*
 * Copyright 2014 Google, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
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

package com.netflix.spinnaker.orca.kato.tasks.gce

import com.netflix.spinnaker.orca.ExecutionStatus
import com.netflix.spinnaker.orca.kato.api.KatoService
import com.netflix.spinnaker.orca.kato.api.TaskId
import com.netflix.spinnaker.orca.pipeline.model.PipelineStage
import spock.lang.Specification
import spock.lang.Subject

class TerminateGoogleInstancesTaskSpec extends Specification {

  @Subject task = new TerminateGoogleInstancesTask()
  def stage = new PipelineStage(type: "whatever")
  def taskId = new TaskId(UUID.randomUUID().toString())

  def terminateInstancesConfig = [
    zone       : "us-central1-b",
    credentials: "fzlem",
    instanceIds: ['i-123456', 'i-654321'],
    launchTimes: [1419273631008, 1419273085007]
  ]

  def setup() {
    stage.context.putAll(terminateInstancesConfig)
  }

  def "creates a terminate google Instance task based on job parameters"() {
    given:
    def operations
    task.kato = Mock(KatoService) {
      1 * requestOperations(*_) >> {
        operations = it[0]
        rx.Observable.from(taskId)
      }
    }

    when:
    task.execute(stage.asImmutable())

    then:
    operations.size() == 1
    with(operations[0].terminateGoogleInstancesDescription) {
      it instanceof Map
      zone == this.terminateInstancesConfig.zone
      credentials == this.terminateInstancesConfig.credentials
      instanceIds == this.terminateInstancesConfig.instanceIds
      launchTimes == this.terminateInstancesConfig.launchTimes
    }
  }

  def "returns a success status with the kato task id"() {
    given:
    task.kato = Stub(KatoService) {
      requestOperations(*_) >> rx.Observable.from(taskId)
    }

    when:
    def result = task.execute(stage.asImmutable())

    then:
    result.status == ExecutionStatus.SUCCEEDED
    result.outputs."kato.task.id" == taskId
    result.outputs."terminate.account.name" == terminateInstancesConfig.credentials
  }
}
