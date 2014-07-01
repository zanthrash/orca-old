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

package com.netflix.spinnaker.orca.bakery.workflow

import groovy.transform.CompileStatic
import com.netflix.spinnaker.orca.bakery.tasks.CreateBakeTask
import com.netflix.spinnaker.orca.bakery.tasks.MonitorBakeTask
import com.netflix.spinnaker.orca.workflow.WorkflowBuilderSupport
import org.springframework.batch.core.job.builder.FlowJobBuilder
import org.springframework.batch.core.job.builder.JobBuilder
import org.springframework.batch.core.job.builder.SimpleJobBuilder
import org.springframework.stereotype.Component

@Component
@CompileStatic
class BakeWorkflowBuilder extends WorkflowBuilderSupport<SimpleJobBuilder> {

  @Override
  SimpleJobBuilder build(JobBuilder jobBuilder) {
    def step1 = steps.get("CreateBakeStep")
      .tasklet(buildTask(CreateBakeTask))
      .build()
    def step2 = steps.get("MonitorBakeStep")
      .tasklet(buildTask(MonitorBakeTask))
      .build()
    jobBuilder
      .start(step1)
      .next(step2)
  }

  @Override
  SimpleJobBuilder build(SimpleJobBuilder jobBuilder) {
    throw new UnsupportedOperationException() // TODO: need to implement this with some tests. It should basically do whe the other method does but use next rather than start
  }
}
