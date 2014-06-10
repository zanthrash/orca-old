package com.netflix.spinnaker.orca.cassandra

import com.google.common.base.Strings
import com.netflix.astyanax.Keyspace
import com.netflix.astyanax.connectionpool.OperationResult
import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.model.CqlResult
import com.netflix.astyanax.serializers.IntegerSerializer
import com.netflix.astyanax.serializers.StringSerializer
import groovy.transform.CompileStatic
import groovy.transform.TupleConstructor
import org.springframework.batch.core.*
import org.springframework.batch.core.launch.NoSuchJobException
import org.springframework.batch.core.repository.dao.JobInstanceDao

@CompileStatic
class AstyanaxJobInstanceDao implements JobInstanceDao {

    private
    static ColumnFamily<Integer, String> CF_BATCH = ColumnFamily.newColumnFamily("batch", IntegerSerializer.get(), StringSerializer.get())

    private final Keyspace keyspace
    private JobKeyGenerator<JobParameters> jobKeyGenerator = new DefaultJobKeyGenerator()

    AstyanaxJobInstanceDao(Keyspace keyspace) {
        this.keyspace = keyspace
    }

    @Override
    JobInstance createJobInstance(String jobName, JobParameters jobParameters) {
        assert jobName, "Job name must not be null."
        assert jobParameters, "JobParameters must not be null."
        assert !getJobInstance(jobName, jobParameters), "JobInstance must not already exist"

        def jobId = UUID.randomUUID().mostSignificantBits // TODO: extract a strategy

        def jobInstance = new JobInstance(jobId, jobName)
        jobInstance.incrementVersion()

        keyspace.prepareQuery(CF_BATCH)
            .withCql("INSERT into JOB_INSTANCE(JOB_INSTANCE_ID, JOB_NAME, JOB_KEY, VERSION) values ($jobId, $jobName, ${jobKeyGenerator.generateKey(jobParameters)}, $jobInstance.version)")
            .execute()

        return jobInstance
    }

    @Override
    JobInstance getJobInstance(String jobName, JobParameters jobParameters) {
        assert jobName, "Job name must not be null."
        assert jobParameters, "JobParameters must not be null."

        def jobKey = jobKeyGenerator.generateKey(jobParameters)

        OperationResult<CqlResult<Integer, String>> result
        if (!Strings.isNullOrEmpty(jobKey)) {
            result = keyspace.prepareQuery(CF_BATCH)
                .withCql("SELECT JOB_INSTANCE_ID, JOB_NAME from JOB_INSTANCE where JOB_NAME = $jobName and JOB_KEY = $jobKey")
                .execute()
        } else {
            result = keyspace.prepareQuery(CF_BATCH)
                .withCql("SELECT JOB_INSTANCE_ID, JOB_NAME from JOB_INSTANCE where JOB_NAME = $jobName and (JOB_KEY = $jobKey OR JOB_KEY is NULL)")
                .execute()
        }

        if (result.result.number == 0) {
            return null
        } else {
            assert result.result.number == 1
            def row = result.result.rows[0]
            def columns = row.columns
            new JobInstance(
                columns.getColumnByName("JOB_INSTANCE_ID").longValue,
                columns.getColumnByName("JOB_NAME").stringValue
            )
        }
    }

    @Override
    JobInstance getJobInstance(Long instanceId) {
        return null
    }

    @Override
    JobInstance getJobInstance(JobExecution jobExecution) {
        return null
    }

    @Override
    List<JobInstance> getJobInstances(String jobName, int start, int count) {
        return null
    }

    @Override
    List<String> getJobNames() {
        return null
    }

    @Override
    List<JobInstance> findJobInstancesByName(String jobName, int start, int count) {
        return null
    }

    @Override
    int getJobInstanceCount(String jobName) throws NoSuchJobException {
        return 0
    }
}
