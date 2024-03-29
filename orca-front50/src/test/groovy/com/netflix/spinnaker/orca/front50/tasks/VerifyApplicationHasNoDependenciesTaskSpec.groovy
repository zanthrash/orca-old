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


package com.netflix.spinnaker.orca.front50.tasks

import com.fasterxml.jackson.databind.ObjectMapper
import com.netflix.spinnaker.orca.ExecutionStatus
import com.netflix.spinnaker.orca.pipeline.model.DefaultTask
import com.netflix.spinnaker.orca.pipeline.model.Pipeline
import com.netflix.spinnaker.orca.pipeline.model.PipelineStage
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class VerifyApplicationHasNoDependenciesTaskSpec extends Specification {
  @Shared
  def config = [account: "test", application: ["name": "application"]]

  @Unroll
  void "should be TERMINAL when application has load balancers, server groups or security groups"() {
    given:
    def fixedAccount = account
    def fixedLoadBalancers = loadBalancers
    def fixedServerGroups = serverGroups
    def fixedSecurityGroups = securityGroups
    def task = new VerifyApplicationHasNoDependenciesTask() {
      @Override
      protected Map getOortResult(String applicationName) {
        return [
          clusters: [
            (fixedAccount): [
              [
                "loadBalancers": fixedLoadBalancers,
                "serverGroups" : fixedServerGroups
              ]
            ]
          ]

        ]
      }

      @Override
      protected List<Map> getMortResults(String applicationName, String type) {
        return fixedSecurityGroups
      }
    }
    task.objectMapper = new ObjectMapper()

    and:
    def stage = new PipelineStage(new Pipeline(), "VerifyApplication", config)
    stage.tasks = [new DefaultTask(name: "T1"), new DefaultTask(name: "T2")]

    when:
    def taskResult = task.execute(stage)

    then:
    taskResult.status == executionStatus

    where:
    account        | loadBalancers     | serverGroups     | securityGroups || executionStatus
    config.account | []                | []               | []             || ExecutionStatus.SUCCEEDED
    config.account | ["loadBalancer1"] | []               | []             || ExecutionStatus.TERMINAL
    config.account | []                | ["serverGroup1"] | []             || ExecutionStatus.TERMINAL
    config.account | []                | []               | [[
                                                               "application": config.application.name,
                                                               "account"    : config.account
                                                             ]]            || ExecutionStatus.TERMINAL
    config.account | ["loadBalancer1"] | ["serverGroup1"] | [[
                                                               "application": config.application.name,
                                                               "account"    : config.account
                                                             ]]            || ExecutionStatus.TERMINAL
    "prod"         | ["loadBalancer1"] | ["serverGroup1"] | [[
                                                               "application": config.application.name,
                                                               "account"    : "prod"
                                                             ]]            || ExecutionStatus.SUCCEEDED
  }
}
