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

package com.netflix.spinnaker.orca.batch

import com.netflix.spinnaker.orca.pipeline.model.OrchestrationStage
import com.netflix.spinnaker.orca.pipeline.model.PipelineStage
import com.netflix.spinnaker.orca.pipeline.model.Stage
import com.netflix.spinnaker.orca.pipeline.persistence.ExecutionRepository

abstract class AbstractStagePropagationListener extends StageExecutionListener {

  AbstractStagePropagationListener(ExecutionRepository executionRepository) {
    super(executionRepository)
  }

  protected void saveStage(Stage stage) {
    if (stage instanceof PipelineStage) {
      executionRepository.storeStage(stage as PipelineStage)
    } else {
      executionRepository.storeStage(stage as OrchestrationStage)
    }
  }
}
