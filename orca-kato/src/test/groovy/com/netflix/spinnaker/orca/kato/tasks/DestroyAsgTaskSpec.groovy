/*
 * Copyright 2014 Netflix, Inc.
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

package com.netflix.spinnaker.orca.kato.tasks

import com.fasterxml.jackson.datatype.guava.GuavaModule
import com.netflix.spinnaker.orca.ExecutionStatus
import com.netflix.spinnaker.orca.jackson.OrcaObjectMapper
import com.netflix.spinnaker.orca.kato.api.KatoService
import com.netflix.spinnaker.orca.kato.api.TaskId
import com.netflix.spinnaker.orca.pipeline.model.Pipeline
import com.netflix.spinnaker.orca.pipeline.model.PipelineStage
import spock.lang.Specification
import spock.lang.Subject

class DestroyAsgTaskSpec extends Specification {
  @Subject task = new DestroyAsgTask()
  def stage = new PipelineStage(new Pipeline(), "whatever")
  def mapper = new OrcaObjectMapper()
  def taskId = new TaskId(UUID.randomUUID().toString())

  def destroyASGConfig = [
    asgName    : "test-asg",
    regions    : ["us-west-1"],
    credentials: "fzlem"
  ]

  def setup() {
    mapper.registerModule(new GuavaModule())

    task.mapper = mapper

    stage.context = destroyASGConfig
  }

  def "creates a destroy ASG task based on job parameters"() {
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
    with(operations[0].destroyAsgDescription) {
      it instanceof Map
      asgName == this.destroyASGConfig.asgName
      regions == this.destroyASGConfig.regions
      credentials == this.destroyASGConfig.credentials
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
    result.outputs."deploy.account.name" == destroyASGConfig.credentials
  }

  void "should pop inputs off destroyAsgDescriptions context field when present"() {
    setup:
    stage.context.destroyAsgDescriptions = [
      [asgName: "foo", regions: ["us"], credentials: account],
      [asgName: "bar", regions: ["us"], credentials: account]
    ]
    task.kato = Stub(KatoService) {
      requestOperations(*_) >> rx.Observable.from(taskId)
    }

    when:
    def result = task.execute(stage.asImmutable())

    then:
    1 == stage.context.destroyAsgDescriptions.size()
    result.outputs."deploy.account.name" == account

    where:
    account = "account"
  }
}
