/*
 * Copyright 2015 Netflix, Inc.
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

package com.netflix.spinnaker.orca.igor.tasks

import com.netflix.spinnaker.orca.DefaultTaskResult
import com.netflix.spinnaker.orca.ExecutionStatus
import com.netflix.spinnaker.orca.Task
import com.netflix.spinnaker.orca.TaskResult
import com.netflix.spinnaker.orca.igor.IgorService
import com.netflix.spinnaker.orca.pipeline.model.Stage
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import retrofit.RetrofitError

@Component
class StartJenkinsJobTask implements Task {

  @Autowired
  IgorService igorService

  @Override
  TaskResult execute(Stage stage) {
    String master = stage.context.master
    String job = stage.context.job
    Map<String,String> parameters = stage.context.parameters

    try {
      Map<String, Object> build = igorService.build(master, job, parameters)
      new DefaultTaskResult(ExecutionStatus.SUCCEEDED, [buildNumber: build.number])
    } catch (RetrofitError e) {
      new DefaultTaskResult(ExecutionStatus.TERMINAL)
    }
  }
}