/*
 * Copyright 2015 Netflix, Inc.
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

package com.netflix.spinnaker.orca.kato.pipeline

import com.netflix.spinnaker.orca.pipeline.model.Pipeline
import com.netflix.spinnaker.orca.pipeline.model.PipelineStage
import spock.lang.Specification
import spock.lang.Unroll

class ParallelDeployStageSpec extends Specification {
  @Unroll
  def "should build contexts corresponding to cluster configuration(s)"() {
    given:
    def bakeStage = new PipelineStage(new Pipeline(), "deploy", "Deploy!", stageContext)
    def builder = new ParallelDeployStage()

    when:
    def parallelContexts = builder.parallelContexts(bakeStage)

    then:
    parallelContexts == expectedParallelContexts

    where:
    stageContext                                            || expectedParallelContexts
    deployStageContext("prod", "us-west-1")                 || [[account: "prod", cluster: [availabilityZones: ["us-west-1": []]], type: "linearDeploy", name: "Deploy in us-west-1"]]
    deployStageContext("prod", "us-west-1", "us-east-1")    || [[account: "prod", cluster: [availabilityZones: ["us-west-1": []]], type: "linearDeploy", name: "Deploy in us-west-1"],
                                                                [account: "prod", cluster: [availabilityZones: ["us-east-1": []]], type: "linearDeploy", name: "Deploy in us-east-1"]]
    [availabilityZones: ["us-west-1": []], account: "prod"] || [[account: "prod", cluster: [availabilityZones: ["us-west-1": []], account: "prod"], type: "linearDeploy", name: "Deploy in us-west-1"]]
  }

  @Unroll
  def "should return stage name regardless of whether parallel flows are present"() {
    given:
    def stage = new PipelineStage(new Pipeline(), "type", stageName, [:])

    expect:
    new ParallelDeployStage().parallelStageName(stage, hasParallelFlows) == expectedStageName

    where:
    stageName | hasParallelFlows || expectedStageName
    "Default" | false            || "Default"
    "Default" | true             || "Default"
  }

  Map deployStageContext(String account, String... availabilityZones) {
    def context = ["account": account]
    if (availabilityZones.size() == 1) {
      context.cluster = ["availabilityZones": [(availabilityZones[0]): []]]
    } else {
      context.clusters = availabilityZones.collect { ["availabilityZones": [(it): []]] }
    }
    return context
  }
}
