package com.netflix.bluespar.orca.bakery.tasks

import com.netflix.bluespar.orca.bakery.api.BakeryService
import groovy.transform.CompileStatic
import org.springframework.batch.core.StepContribution
import org.springframework.batch.core.scope.context.ChunkContext
import org.springframework.batch.core.step.tasklet.Tasklet
import org.springframework.batch.repeat.RepeatStatus
import org.springframework.beans.factory.annotation.Autowired

import static org.springframework.batch.repeat.RepeatStatus.FINISHED

@CompileStatic
class CreateBakeTask implements Tasklet {

    @Autowired
    BakeryService bakery

    @Override
    RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        def bakeStatus = bakery.createBake(chunkContext.stepContext.jobParameters.region as String)
        chunkContext.stepContext.stepExecution.jobExecution.executionContext.with {
            put("bake.status", bakeStatus)
        }
        return FINISHED
    }
}
