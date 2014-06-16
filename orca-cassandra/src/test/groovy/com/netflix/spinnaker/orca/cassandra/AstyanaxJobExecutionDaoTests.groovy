package com.netflix.spinnaker.orca.cassandra

import com.netflix.astyanax.Keyspace
import com.netflix.astyanax.connectionpool.OperationResult
import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.model.CqlResult
import com.netflix.astyanax.serializers.IntegerSerializer
import com.netflix.astyanax.serializers.StringSerializer
import com.netflix.spinnaker.orca.cassandra.config.CassandraConfig
import groovy.transform.CompileStatic
import org.junit.After
import org.junit.runner.RunWith
import org.springframework.batch.core.repository.dao.AbstractJobExecutionDaoTests
import org.springframework.batch.core.repository.dao.JobExecutionDao
import org.springframework.batch.core.repository.dao.JobInstanceDao
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner

@RunWith(SpringJUnit4ClassRunner)
@ContextConfiguration(classes = [CassandraConfig])
@CompileStatic
class AstyanaxJobExecutionDaoTests extends AbstractJobExecutionDaoTests {

    @Autowired
    private Keyspace keyspace
    private ColumnFamily<Integer, String> columnFamily = ColumnFamily.newColumnFamily("batch", IntegerSerializer.get(), StringSerializer.get())

    @After
    public void destroyData() {
        execute "truncate batch_job_execution"
        execute "truncate batch_job_instance"
    }

    @Override
    protected JobExecutionDao getJobExecutionDao() {
        new AstyanaxJobExecutionDao(keyspace, null)
    }

    @Override
    protected JobInstanceDao getJobInstanceDao() {
        new AstyanaxJobInstanceDao(keyspace)
    }

    private OperationResult<CqlResult<Integer, String>> execute(String cql) {
        keyspace.prepareQuery(columnFamily)
            .withCql(cql)
            .execute()
    }
}
