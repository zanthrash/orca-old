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


package com.netflix.spinnaker.orca.kato.pipeline
import groovy.transform.CompileStatic
import com.netflix.spinnaker.orca.kato.tasks.DeleteSecurityGroupForceRefreshTask
import com.netflix.spinnaker.orca.kato.tasks.DeleteSecurityGroupTask
import com.netflix.spinnaker.orca.kato.tasks.MonitorKatoTask
import com.netflix.spinnaker.orca.pipeline.LinearStage
import com.netflix.spinnaker.orca.pipeline.model.Stage
import org.springframework.batch.core.Step
import org.springframework.stereotype.Component

@Component
@CompileStatic
class DeleteSecurityGroupStage extends LinearStage {

  public static final String MAYO_CONFIG_TYPE = "deleteSecurityGroup"

  DeleteSecurityGroupStage() {
    super(MAYO_CONFIG_TYPE)
  }

  @Override
  public List<Step> buildSteps(Stage stage) {
    [
      buildStep(stage, MAYO_CONFIG_TYPE, DeleteSecurityGroupTask),
      buildStep(stage, "forceCacheRefresh", DeleteSecurityGroupForceRefreshTask),
      buildStep(stage, "monitorDelete", MonitorKatoTask),
    ]
  }
}
