package com.netflix.spinnaker.orca.cassandra

import com.google.common.base.Strings
import com.netflix.astyanax.Keyspace
import com.netflix.astyanax.connectionpool.OperationResult
import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.model.CqlResult
import com.netflix.astyanax.serializers.IntegerSerializer
import com.netflix.astyanax.serializers.StringSerializer
import com.netflix.astyanax.util.TimeUUIDUtils
import groovy.transform.CompileStatic
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

        def jobId = TimeUUIDUtils.getTimeFromUUID(UUID.randomUUID()) // TODO: extract a strategy

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
        mapSingleJobInstanceResult(result)
    }

    @Override
    JobInstance getJobInstance(Long instanceId) {
        def result = keyspace.prepareQuery(CF_BATCH)
            .withCql("SELECT JOB_INSTANCE_ID, JOB_NAME, JOB_KEY, VERSION from JOB_INSTANCE where JOB_INSTANCE_ID = $instanceId") // TODO: why does it select JOB_KEY and VERSION & then not use them?
            .execute()
        mapSingleJobInstanceResult(result)
    }

    @Override
    JobInstance getJobInstance(JobExecution jobExecution) {
        def result = keyspace.prepareQuery(CF_BATCH)
            .withCql("SELECT ji.JOB_INSTANCE_ID, JOB_NAME, JOB_KEY, ji.VERSION from JOB_INSTANCE ji, JOB_EXECUTION je where JOB_EXECUTION_ID = $jobExecution.id and ji.JOB_INSTANCE_ID = je.JOB_INSTANCE_ID") // TOOD: problematic in CQL?
            .execute()
        mapSingleJobInstanceResult(result)
    }

    @Override
    List<JobInstance> getJobInstances(String jobName, int start, int count) {
        def result = keyspace.prepareQuery(CF_BATCH)
            .withCql("SELECT JOB_INSTANCE_ID, JOB_NAME from JOB_INSTANCE where JOB_NAME = $jobName order by JOB_INSTANCE_ID desc")
            .execute()
        mapJobInstancesFromResults(result, start, count)
    }

    @Override
    List<String> getJobNames() {
        def result = keyspace.prepareQuery(CF_BATCH)
            .withCql("SELECT distinct JOB_NAME from JOB_INSTANCE order by JOB_NAME")
            .execute()
        (0..<result.result.rows.size()).collect { int i ->
            def row = result.result.rows[i]
            def columns = row.columns
            columns.getColumnByName("JOB_NAME").stringValue
        }
    }

    @Override
    List<JobInstance> findJobInstancesByName(String jobName, int start, int count) {
        if (jobName.contains("*")) {
            jobName = jobName.replaceAll(/\*/, "%");
        }
        def result = keyspace.prepareQuery(CF_BATCH)
            .withCql("SELECT JOB_INSTANCE_ID, JOB_NAME from JOB_INSTANCE where JOB_NAME like $jobName order by JOB_INSTANCE_ID desc")
            .execute()
        mapJobInstancesFromResults(result, start, count)
    }

    @Override
    int getJobInstanceCount(String jobName) throws NoSuchJobException {
        def result = keyspace.prepareQuery(CF_BATCH)
            .withCql("SELECT COUNT(*) from JOB_INSTANCE where JOB_NAME = $jobName")
            .execute()

        if (result.result.number == 0) {
            return 0
        } else {
            assert result.result.number == 1
            def row = result.result.rows[0]
            row.columns.getColumnByIndex(0).integerValue // TODO: there's probably a better way to do this
        }
    }

    private List<JobInstance> mapJobInstancesFromResults(OperationResult<CqlResult<Integer, String>> result, int start, int count) {
        (start..<[start + count, result.result.rows.size()].min()).collect { int i -> // TODO: this seems naive can we do where rownum >= start limit count?
            mapJobInstanceFromRow(result, i)
        }
    }

    private JobInstance mapSingleJobInstanceResult(OperationResult<CqlResult<Integer, String>> result) {
        if (result.result.number == 0) {
            return null
        } else {
            assert result.result.number == 1
            mapJobInstanceFromRow(result, 0)
            // TODO: should increment according to JobInstanceRowMapper
        }
    }

    private JobInstance mapJobInstanceFromRow(OperationResult<CqlResult<Integer, String>> result, int i) {
        def row = result.result.rows[i]
        def columns = row.columns
        new JobInstance(
            columns.getColumnByName("JOB_INSTANCE_ID").longValue,
            columns.getColumnByName("JOB_NAME").stringValue
        )
    }
}
