package com.netflix.spinnaker.orca.cassandra

import com.netflix.astyanax.Keyspace
import com.netflix.spinnaker.orca.cassandra.config.CassandraConfig
import groovy.transform.CompileStatic
import org.junit.Before
import org.junit.Test
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

    @Override
    protected JobInstanceDao getJobInstanceDao() {
        new AstyanaxJobInstanceDao(keyspace)
    }
}
