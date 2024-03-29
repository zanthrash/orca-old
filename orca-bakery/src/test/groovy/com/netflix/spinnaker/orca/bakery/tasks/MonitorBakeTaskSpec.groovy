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

package com.netflix.spinnaker.orca.bakery.tasks

import com.netflix.spinnaker.orca.ExecutionStatus
import com.netflix.spinnaker.orca.bakery.api.BakeStatus
import com.netflix.spinnaker.orca.bakery.api.BakeryService
import com.netflix.spinnaker.orca.pipeline.model.Pipeline
import com.netflix.spinnaker.orca.pipeline.model.PipelineStage
import rx.Observable
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Subject
import spock.lang.Unroll
import static java.util.UUID.randomUUID

class MonitorBakeTaskSpec extends Specification {

  @Subject
    task = new MonitorBakeTask()

  @Shared
  Pipeline pipeline = new Pipeline()

  @Unroll
  def "should return #taskStatus if bake is #bakeState"() {
    given:
    def previousStatus = new BakeStatus(id: id, state: BakeStatus.State.PENDING)
    def stage = new PipelineStage(pipeline, "bake", [region: "us-west-1", status: previousStatus]).asImmutable()

    and:
    task.bakery = Stub(BakeryService) {
      lookupStatus(stage.context.region, id) >> Observable.from(new BakeStatus(id: id, state: bakeState, result: bakeResult))
    }

    expect:
    task.execute(stage).status == taskStatus

    where:
    bakeState                  | bakeResult                || taskStatus
    BakeStatus.State.PENDING   | null                      || ExecutionStatus.RUNNING
    BakeStatus.State.RUNNING   | null                      || ExecutionStatus.RUNNING
    BakeStatus.State.COMPLETED | BakeStatus.Result.SUCCESS || ExecutionStatus.SUCCEEDED
    BakeStatus.State.COMPLETED | BakeStatus.Result.FAILURE || ExecutionStatus.FAILED
    BakeStatus.State.COMPLETED | null                      || ExecutionStatus.FAILED
    BakeStatus.State.CANCELLED | null                      || ExecutionStatus.FAILED
    BakeStatus.State.SUSPENDED | null                      || ExecutionStatus.RUNNING

    id = randomUUID().toString()
  }

  def "outputs the updated bake status"() {
    given:
    def previousStatus = new BakeStatus(id: id, state: BakeStatus.State.PENDING)
    def stage = new PipelineStage(pipeline, "bake", [region: "us-west-1", status: previousStatus]).asImmutable()

    and:
    task.bakery = Stub(BakeryService) {
      lookupStatus(stage.context.region, id) >> Observable.from(new BakeStatus(id: id, state: BakeStatus.State.COMPLETED))
    }

    when:
    def result = task.execute(stage)

    then:
    with(result.outputs.status) {
      id == previousStatus.id
      state == BakeStatus.State.COMPLETED
    }

    where:
    id = randomUUID().toString()
  }

}
