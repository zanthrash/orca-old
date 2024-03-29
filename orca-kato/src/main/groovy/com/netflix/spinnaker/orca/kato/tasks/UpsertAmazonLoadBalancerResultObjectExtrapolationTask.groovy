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

import com.netflix.spinnaker.orca.DefaultTaskResult
import com.netflix.spinnaker.orca.ExecutionStatus
import com.netflix.spinnaker.orca.Task
import com.netflix.spinnaker.orca.TaskResult
import com.netflix.spinnaker.orca.kato.api.TaskId
import com.netflix.spinnaker.orca.pipeline.model.Stage
import org.springframework.stereotype.Component

@Component
class UpsertAmazonLoadBalancerResultObjectExtrapolationTask implements Task {

  @Override
  TaskResult execute(Stage stage) {
    TaskId lastTaskId = stage.context."kato.last.task.id" as TaskId
    def katoTasks = stage.context."kato.tasks" as List<Map>
    def lastKatoTask = katoTasks.find { it.id.toString() == lastTaskId.id }

    if (!lastKatoTask) {
      return new DefaultTaskResult(ExecutionStatus.FAILED)
    }

    def resultObjects = lastKatoTask.resultObjects as List<Map>
    def resultObjectMap = resultObjects?.getAt(0) as Map
    Map outputs = [:]
    if (resultObjectMap) {
      def dnsName = resultObjectMap.loadBalancers.entrySet().getAt(0).value.dnsName
      outputs["dnsName"] = dnsName
    }

    return new DefaultTaskResult(ExecutionStatus.SUCCEEDED, outputs)
  }

}
