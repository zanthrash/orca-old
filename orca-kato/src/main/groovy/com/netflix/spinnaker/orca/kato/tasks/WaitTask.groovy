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

import groovy.transform.CompileStatic
import java.util.concurrent.TimeUnit
import com.netflix.spinnaker.orca.DefaultTaskResult
import com.netflix.spinnaker.orca.RetryableTask
import com.netflix.spinnaker.orca.TaskResult
import com.netflix.spinnaker.orca.pipeline.model.Stage
import org.springframework.stereotype.Component
import static com.netflix.spinnaker.orca.ExecutionStatus.RUNNING
import static com.netflix.spinnaker.orca.ExecutionStatus.SUCCEEDED

@Component
@CompileStatic
class WaitTask implements RetryableTask {
  long backoffPeriod = 1000
  long timeout = Integer.MAX_VALUE

  TimeProvider timeProvider = new TimeProvider()

  @Override
  TaskResult execute(Stage stage) {
    // wait time is specified in seconds
    long waitTime = stage.context.waitTime as long
    def waitTimeMs = TimeUnit.MILLISECONDS.convert(waitTime, TimeUnit.SECONDS)
    def now = timeProvider.millis

    if (!stage.context.containsKey("waitTaskState") || !stage.context.waitTaskState instanceof Map) {
      new DefaultTaskResult(RUNNING, [waitTaskState: [startTime: now]])
    } else if (now - ((Long) ((Map) stage.context.waitTaskState).startTime) > waitTimeMs) {
      new DefaultTaskResult(SUCCEEDED)
    } else {
      new DefaultTaskResult(RUNNING)
    }
  }

  static class TimeProvider {
    long millis

    long getMillis() {
      this.millis ?: System.currentTimeMillis()
    }
  }
}
