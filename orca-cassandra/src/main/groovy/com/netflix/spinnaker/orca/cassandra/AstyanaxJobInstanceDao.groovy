package com.netflix.spinnaker.orca.cassandra

import com.google.common.base.Strings
import com.netflix.astyanax.Keyspace
import com.netflix.astyanax.connectionpool.OperationResult
import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.model.CqlResult
import com.netflix.astyanax.serializers.IntegerSerializer
import com.netflix.astyanax.serializers.StringSerializer
import groovy.transform.CompileStatic
import org.springframework.batch.core.*
import org.springframework.batch.core.launch.NoSuchJobException
import org.springframework.batch.core.repository.dao.JobInstanceDao

@CompileStatic
class AstyanaxJobInstanceDao implements JobInstanceDao {

    private final Keyspace keyspace
    private final IdentifierGenerator idGenerator = new UUIDIdentifierGenerator()
    private final JobKeyGenerator<JobParameters> jobKeyGenerator = new DefaultJobKeyGenerator()
    // TODO: don't get why we need this when we're querying whatever random table with CQL
    private
    final ColumnFamily<Integer, String> columnFamily = ColumnFamily.newColumnFamily("batch", IntegerSerializer.get(), StringSerializer.get())

    AstyanaxJobInstanceDao(Keyspace keyspace) {
        this.keyspace = keyspace
    }

    @Override
    JobInstance createJobInstance(String jobName, JobParameters jobParameters) {
        assert jobName, "Job name must not be null."
        assert jobParameters, "JobParameters must not be null."
        if (getJobInstance(jobName, jobParameters)) {
            throw new IllegalStateException("JobInstance must not already exist")
        }

        def jobId = idGenerator.next()

        def jobInstance = new JobInstance(jobId, jobName)
        jobInstance.incrementVersion()

        def jobKey = jobKeyGenerator.generateKey(jobParameters)

        keyspace.prepareQuery(columnFamily)
//            .withCql("INSERT into BATCH_JOB_INSTANCE(JOB_INSTANCE_ID, JOB_NAME, JOB_KEY, VERSION) values (?, ?, ?, ?)")
            .withCql("insert into batch_job_instance(job_instance_id, job_name, job_key, version) values ($jobId, '$jobName', '$jobKey', $jobInstance.version)")
//            .asPreparedStatement()
//            .withLongValue(jobId)
//            .withStringValue(jobName)
//            .withStringValue(key)
//            .withIntegerValue(jobInstance.version)
            .execute()

        return jobInstance
    }

    @Override
    JobInstance getJobInstance(String jobName, JobParameters jobParameters) {
        assert jobName, "Job name must not be null."
        assert jobParameters, "JobParameters must not be null."

        def jobKey = jobKeyGenerator.generateKey(jobParameters)

        if (Strings.isNullOrEmpty(jobKey)) {
            def result = keyspace.prepareQuery(columnFamily)
                .withCql("select job_instance_id, job_name from batch_job_instance where job_name = ? and job_key is null allow filtering")
                .asPreparedStatement()
                .withStringValue(jobName)
                .execute()
            mapSingleJobInstanceResult(result)
        } else {
            def result = keyspace.prepareQuery(columnFamily)
                .withCql("select job_instance_id, job_name from batch_job_instance where job_name = ? and job_key = ? allow filtering")
                .asPreparedStatement()
                .withStringValue(jobName)
                .withStringValue(jobKey)
                .execute()
            mapSingleJobInstanceResult(result)
        }
    }

    @Override
    JobInstance getJobInstance(Long instanceId) {
        def result = keyspace.prepareQuery(columnFamily)
            .withCql("select job_instance_id, job_name, job_key, version from batch_job_instance where job_instance_id = ?") // TODO: why does it select JOB_KEY and VERSION & then not use them?
            .asPreparedStatement()
            .withLongValue(instanceId)
            .execute()
        mapSingleJobInstanceResult(result)
    }

    @Override
    JobInstance getJobInstance(JobExecution jobExecution) {
        def result = keyspace.prepareQuery(columnFamily)
            .withCql("select job_instance_id from batch_job_execution where job_execution_id = ?")
            .asPreparedStatement()
            .withLongValue(jobExecution.id)
            .execute()
        if (result.result.number == 0) {
            return null
        } else {
            assert result.result.number == 1
            def row = result.result.rows[0]
            getJobInstance(row.columns.getColumnByIndex(0).longValue)
        }
    }

    @Override
    List<JobInstance> getJobInstances(String jobName, int start, int count) {
        def result = keyspace.prepareQuery(columnFamily)
            .withCql("select job_instance_id, job_name from batch_job_instance where job_name = ? /*order by job_instance_id desc*/")
            .asPreparedStatement()
            .withStringValue(jobName)
            .execute()
        mapJobInstancesFromResults(result, start, count)
    }

    @Override
    List<String> getJobNames() {
        def result = keyspace.prepareQuery(columnFamily)
            .withCql("select /*distinct*/ job_name from batch_job_instance /*order by job_name*/")
            .execute()
        (0..<result.result.rows.size()).collect { int i ->
            def row = result.result.rows[i]
            def columns = row.columns
            columns.getColumnByName("job_name").stringValue
        }.unique().sort()
        // select distinct not supported until CQL 3.11, order by doesn't work unless you have a restriction
    }

    @Override
    List<JobInstance> findJobInstancesByName(String jobName, int start, int count) {
        if (jobName.contains("*")) {
            jobName = jobName.replaceAll(/\*/, "%");
        }
        def result = keyspace.prepareQuery(columnFamily)
            .withCql("select job_instance_id, job_name from batch_job_instance where job_name /*like*/ = ? /*order by job_instance_id desc*/") // can't do like. May be able to do a slice query if we really need it, see http://doanduyhai.wordpress.com/2012/07/05/apache-cassandra-tricks-and-traps/
            .asPreparedStatement()
            .withStringValue(jobName)
            .execute()
        mapJobInstancesFromResults(result, start, count)
    }

    @Override
    int getJobInstanceCount(String jobName) throws NoSuchJobException {
        def result = keyspace.prepareQuery(columnFamily)
            .withCql("select count(*) from batch_job_instance where job_name = ?")
            .asPreparedStatement()
            .withStringValue(jobName)
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
        if (start >= result.result.rows.size()) {
            return []
        }
        (start..<[start + count, result.result.rows.size()].min()).collect { int i -> // TODO: this seems naive can we do where rownum >= start limit count?
            mapJobInstanceFromRow(result, i)
        }
    }

    private JobInstance mapSingleJobInstanceResult(OperationResult<CqlResult<Integer, String>> result) {
        if (!result.result.hasRows() || result.result.rows.size() == 0) {
            return null
        } else {
            assert result.result.rows.size() == 1
            mapJobInstanceFromRow(result, 0)
            // TODO: should increment according to JobInstanceRowMapper
        }
    }

    private JobInstance mapJobInstanceFromRow(OperationResult<CqlResult<Integer, String>> result, int i) {
        if (i > result.result.rows.size()) {
            return null
        }
        def row = result.result.rows[i]
        def columns = row.columns
        def jobInstance = new JobInstance(
            columns.getColumnByName("job_instance_id").longValue,
            columns.getColumnByName("job_name").stringValue
        )
        jobInstance.incrementVersion() // version is never updated - see JdbcJobInstanceDao.JobInstanceRowMapper.mapRow
        return jobInstance
    }
}
