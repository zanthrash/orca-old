package com.netflix.spinnaker.orca.cassandra

import com.google.common.base.Optional
import com.netflix.astyanax.Keyspace
import com.netflix.astyanax.connectionpool.OperationResult
import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.model.ColumnList
import com.netflix.astyanax.model.CqlResult
import com.netflix.astyanax.serializers.IntegerSerializer
import com.netflix.astyanax.serializers.StringSerializer
import groovy.transform.CompileStatic
import org.springframework.batch.core.*
import org.springframework.batch.core.repository.dao.JobExecutionDao
import org.springframework.batch.core.repository.dao.JobInstanceDao
import org.springframework.batch.core.repository.dao.NoSuchObjectException
import org.springframework.dao.OptimisticLockingFailureException

@CompileStatic
class AstyanaxJobExecutionDao implements JobExecutionDao {

    private final Keyspace keyspace
    private final IdentifierGenerator idGenerator = new UUIDIdentifierGenerator()
    // TODO: don't get why we need this when we're querying whatever random table with CQL
    private
    final ColumnFamily<Integer, String> columnFamily = ColumnFamily.newColumnFamily("batch", IntegerSerializer.get(), StringSerializer.get())
    private final JobInstanceDao jobInstanceDao

    AstyanaxJobExecutionDao(Keyspace keyspace, JobInstanceDao jobInstanceDao) {
        this.keyspace = keyspace
        this.jobInstanceDao = jobInstanceDao
    }

    @Override
    void saveJobExecution(JobExecution jobExecution) {
        validateJobExecution jobExecution

        jobExecution.incrementVersion()

        jobExecution.id = idGenerator.next()
        keyspace.prepareQuery(columnFamily)
            .withCql("""insert into batch_job_execution(job_execution_id, job_instance_id, start_time, end_time, ended,
                                                        status, exit_code, exit_message, version, create_time,
                                                        last_updated, job_configuration_location)
                                                values ($jobExecution.id, $jobExecution.jobId,
                                                        ${jobExecution.startTime?.time},
                                                        ${jobExecution.endTime?.time},
                                                        ${jobExecution.endTime != null},
                                                        '$jobExecution.status', '$jobExecution.exitStatus.exitCode',
                                                        '$jobExecution.exitStatus.exitDescription',
                                                        $jobExecution.version,
                                                        $jobExecution.createTime.time,
                                                        ${jobExecution.lastUpdated?.time},
                                                        $jobExecution.jobConfigurationName)""")
//            .asPreparedStatement()
//            .withLongValue(jobExecution.id)
//            .withLongValue(jobExecution.jobId)
//            .withLongValue(jobExecution.startTime?.time)
//            .withLongValue(jobExecution.endTime?.time)
//            .withStringValue(jobExecution.status.toString())
//            .withStringValue(jobExecution.exitStatus.exitCode)
//            .withStringValue(jobExecution.exitStatus.exitDescription)
//            .withIntegerValue(jobExecution.version)
//            .withLongValue(jobExecution.createTime.time)
//            .withLongValue(jobExecution.lastUpdated?.time)
//            .withStringValue(jobExecution.jobConfigurationName)
        // TODO: wanted to use prepared query but it doesn't like nulls being passed to .withStringValue, etc.
            .execute()

        insertJobParameters(jobExecution.getId(), jobExecution.getJobParameters())
    }

    @Override
    void updateJobExecution(JobExecution jobExecution) {
        validateJobExecution jobExecution

        assert jobExecution.id != null, "JobExecution ID cannot be null. JobExecution must be saved before it can be updated"

        assert jobExecution.version, "JobExecution version cannot be null. JobExecution must be saved before it can be updated"

        synchronized (jobExecution) {
            def version = jobExecution.version + 1

            checkJobExecutionIdExists(jobExecution)

            def result = keyspace.prepareQuery(columnFamily)
                .withCql("""update batch_job_execution set start_time = ?, end_time = ?, ended = ?, status = ?, exit_code = ?,
                                                           exit_message = ?, version = ?, create_time = ?,
                                                           last_updated = ?
                                                     where job_execution_id = ? and version = ?""")
                .asPreparedStatement()
                .withLongValue(jobExecution.startTime?.time)
                .withLongValue(jobExecution.endTime?.time)
                .withBooleanValue(jobExecution.endTime != null)
                .withStringValue(jobExecution.status.toString())
                .withStringValue(jobExecution.exitStatus.exitCode)
                .withStringValue(jobExecution.exitStatus.exitDescription)
                .withIntegerValue(jobExecution.version)
                .withLongValue(jobExecution.createTime.time)
                .withLongValue(jobExecution.lastUpdated?.time)
                .withLongValue(jobExecution.id)
                .withIntegerValue(jobExecution.version)
                .execute()

            if (result.result.number == 0) {
                def currentVersion = findCurrentJobExecutionVersion(jobExecution)
                throw new OptimisticLockingFailureException("Attempt to update job execution id=$jobExecution.id with wrong version ($jobExecution.version), where current version is $currentVersion")
            }

            jobExecution.incrementVersion()
        }
    }

    @Override
    List<JobExecution> findJobExecutions(JobInstance job) {
        assert job != null, "Job cannot be null."
        assert job.id != null, "Job Id cannot be null."

        def result = keyspace.prepareQuery(columnFamily)
            .withCql("""select job_execution_id, job_instance_id, start_time, end_time, status, exit_code, exit_message, create_time,
                           last_updated, version, job_configuration_location
                      from batch_job_execution
                     where job_instance_id = ?
                  /* order by job_execution_id desc */
                     allow filtering""") // TODO: order by not supported on secondary index
            .asPreparedStatement()
            .withLongValue(job.id)
            .execute()

        (0..<result.result.rows.size()).collect { int i ->
            mapJobExecutionFromRow(result, i, job)
        }
    }

    @Override
    JobExecution getLastJobExecution(JobInstance jobInstance) {
        return null
    }

    @Override
    Set<JobExecution> findRunningJobExecutions(String jobName) {
        def jobInstances = jobInstanceDao.getJobInstances(jobName, 0, Integer.MAX_VALUE)

        def result = keyspace.prepareQuery(columnFamily)
            .withCql("""select job_execution_id, job_instance_id, start_time, end_time, status, exit_code, exit_message,
                           create_time, last_updated, version, job_instance_id, job_configuration_location
                      from batch_job_execution
                     where job_instance_id in (${jobInstances.id.join(", ")})
                       and ended = false
                  /* order by e.job_execution_id desc*/""")
        .execute()

        (0..<result.result.rows.size()).collect { int i ->
            mapJobExecutionFromRow(result, i)
        } as Set
    }

    @Override
    JobExecution getJobExecution(Long executionId) {
        return null
    }

    @Override
    void synchronizeStatus(JobExecution jobExecution) {

    }

    private void validateJobExecution(JobExecution jobExecution) {
        assert jobExecution != null
        assert jobExecution.jobId != null, "JobExecution Job-Id cannot be null."
        assert jobExecution.status != null, "JobExecution status cannot be null."
        assert jobExecution.createTime != null, "JobExecution create time cannot be null"
    }

    private void insertJobParameters(Long executionId, JobParameters jobParameters) {
        jobParameters.parameters.entrySet().each { entry ->
            JobParameter jobParameter = entry.value
            insertParameter(executionId, jobParameter.type, entry.key, jobParameter.value, jobParameter.identifying)
        }
    }

    private void insertParameter(Long executionId, JobParameter.ParameterType type, String key, Object value, boolean identifying) {
//        Object[] args = new Object[0]
//        int[] argTypes = new int[] { Types.BIGINT, Types.VARCHAR,
//            Types.VARCHAR, Types.VARCHAR, Types.TIMESTAMP, Types.BIGINT,
//            Types.DOUBLE, Types.CHAR }
//
//        String identifyingFlag = identifying? "Y":"N"
//
//        if (type == JobParameter.ParameterType.STRING) {
//            args = new Object[] { executionId, key, type, value, new Timestamp(0L),
//                0L, 0D, identifyingFlag}
//        } else if (type == JobParameter.ParameterType.LONG) {
//            args = new Object[] { executionId, key, type, "", new Timestamp(0L),
//                value, new Double(0), identifyingFlag}
//        } else if (type == JobParameter.ParameterType.DOUBLE) {
//            args = new Object[] { executionId, key, type, "", new Timestamp(0L), 0L,
//                value, identifyingFlag}
//        } else if (type == JobParameter.ParameterType.DATE) {
//            args = new Object[] { executionId, key, type, "", value, 0L, 0D, identifyingFlag}
//        }
//
//        getJdbcTemplate().update(getQuery(CREATE_JOB_PARAMETERS), args, argTypes)
    }

    private void checkJobExecutionIdExists(JobExecution jobExecution) {
        def result = keyspace.prepareQuery(columnFamily)
            .withCql("select count(*) from batch_job_execution where job_execution_id = ?")
            .asPreparedStatement()
            .withLongValue(jobExecution.id)
            .execute()
        if (result.result.number == 0) {
            throw new NoSuchObjectException("Invalid JobExecution, ID $jobExecution.id not found.")
        }
    }

    private int findCurrentJobExecutionVersion(JobExecution jobExecution) {
        def result = keyspace.prepareQuery(columnFamily)
            .withCql("select version from batch_job_execution where job_execution_id = ?")
            .asPreparedStatement()
            .withLongValue(jobExecution.id)
            .execute()
        def row = result.result.rows[0]
        def columns = row.columns
        columns.getColumnByName("version").integerValue
    }

    private JobExecution mapJobExecutionFromRow(OperationResult<CqlResult<Integer, String>> result, int i, JobInstance jobInstance) {
        mapJobExecutionFromRow(result, i, Optional.of(jobInstance))
    }

    private JobExecution mapJobExecutionFromRow(OperationResult<CqlResult<Integer, String>> result, int i) {
        mapJobExecutionFromRow(result, i, Optional.absent())
    }

    private JobExecution mapJobExecutionFromRow(OperationResult<CqlResult<Integer, String>> result, int i, Optional<JobInstance> jobInstance) {
        if (i > result.result.rows.size()) {
            return null
        }
        def row = result.result.rows[i]
        def columns = row.columns

        if (!jobInstance.present) {
            def jobInstanceId = columns.getColumnByName("job_instance_id").longValue
            jobInstance = Optional.of(jobInstanceDao.getJobInstance(jobInstanceId))
        }

        def jobExecutionId = columns.getColumnByName("job_execution_id").longValue
        def jobParameters = getJobParameters(jobExecutionId)
        def jobConfigurationLocation = getStringValueOrNull(columns, "job_configuration_location")
        def jobExecution = new JobExecution(jobInstance.get(), jobExecutionId, jobParameters, jobConfigurationLocation)

        jobExecution.setStartTime(getDateValueOrNull(columns, "start_time"))
        jobExecution.setEndTime(getDateValueOrNull(columns, "end_time"))
        jobExecution.setStatus(BatchStatus.valueOf(columns.getColumnByName("status").stringValue))
        jobExecution.setExitStatus(new ExitStatus(columns.getColumnByName("exit_code").stringValue, getStringValueOrNull(columns, "exit_message")))
        jobExecution.setCreateTime(getDateValueOrNull(columns, "create_time"))
        jobExecution.setLastUpdated(getDateValueOrNull(columns, "last_updated"))
        jobExecution.setVersion(getIntegerValueOrNull(columns, "version"))
        return jobExecution
    }

    private String getStringValueOrNull(ColumnList<String> columns, String columnName) {
        def column = columns.getColumnByName(columnName)
        column.hasValue() ? column.stringValue : null
    }

    private int getIntegerValueOrNull(ColumnList<String> columns, String columnName) {
        def column = columns.getColumnByName(columnName)
        /*column.hasValue() ? column.integerValue :*/ 0
        // TODO : this throws spurious seeming NPE. Looks like the column contains 8 x hex 0 bytes
    }

    private Date getDateValueOrNull(ColumnList<String> columns, String columnName) {
        def column = columns.getColumnByName(columnName)
        column.hasValue() ? column.dateValue : null
    }

    private JobParameters getJobParameters(long jobExecutionId) {
        new JobParameters() // TODO: I'm sure we can just dump /read this as JSON
    }
}
