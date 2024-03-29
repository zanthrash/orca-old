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

package com.netflix.spinnaker.orca.pipeline

import groovy.transform.CompileStatic
import javax.annotation.PostConstruct
import com.google.common.collect.ImmutableList
import com.netflix.spinnaker.orca.batch.StageBuilder
import com.netflix.spinnaker.orca.pipeline.model.Execution
import org.springframework.batch.core.Job
import org.springframework.batch.core.JobExecutionListener
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationContext

/**
 * Builds a Spring Batch `Job` from an orchestration.
 */
@CompileStatic
abstract class ExecutionJobBuilder<T extends Execution> {

  @Autowired protected ApplicationContext applicationContext
  @Autowired protected JobBuilderFactory jobs
  @Autowired protected StepBuilderFactory steps
  protected List<JobExecutionListener> pipelineListeners

  protected final Map<String, StageBuilder> stages = [:]

  boolean isValidStage(String name) {
    stages.containsKey(name)
  }

  @PostConstruct
  void initialize() {
    applicationContext.getBeansOfType(StageBuilder).values().each {
      stages[it.type] = it
    }
    applicationContext.getBeansOfType(StandaloneTask).values().each {
      def stage = new SimpleStage(it.type, it)
      applicationContext.autowireCapableBeanFactory.autowireBean(stage)
      // TODO: this should be a prototype scoped bean or use a factory I guess
      stages[it.type] = stage
    }
  }

  abstract Job build(T subject)

  abstract String jobNameFor(T subject)

  @Autowired(required = false)
  void setPipelineListeners(List<JobExecutionListener> pipelineListeners) {
    this.pipelineListeners = pipelineListeners
  }

  protected List<JobExecutionListener> getPipelineListeners() {
    def listBuilder = ImmutableList.builder()
    if (pipelineListeners) {
      listBuilder.addAll(pipelineListeners)
    }
    listBuilder.build()
  }
}
