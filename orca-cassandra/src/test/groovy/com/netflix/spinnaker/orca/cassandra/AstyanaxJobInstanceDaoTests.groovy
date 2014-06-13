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
import org.junit.Before
import org.junit.BeforeClass
import org.junit.runner.RunWith
import org.springframework.batch.core.repository.dao.AbstractJobInstanceDaoTests
import org.springframework.batch.core.repository.dao.JobInstanceDao
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner

@RunWith(SpringJUnit4ClassRunner)
@ContextConfiguration(classes = [CassandraConfig])
@CompileStatic
class AstyanaxJobInstanceDaoTests extends AbstractJobInstanceDaoTests {

    @Autowired
    private Keyspace keyspace
    private ColumnFamily<Integer, String> columnFamily = ColumnFamily.newColumnFamily("batch", IntegerSerializer.get(), StringSerializer.get())

//    @Before
//    public void createSchema() {
//        execute "create table batch_job_instance (job_instance_id bigint primary key, version bigint, job_name varchar, job_key varchar)"
//        execute "create index on batch_job_instance (job_name)"
//        execute "create index on batch_job_instance (job_key)"
//    }
//
    @After
    public void destroyData() {
        execute "truncate batch_job_instance"
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
