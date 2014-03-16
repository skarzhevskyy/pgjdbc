package org.postgresql.test.other;

import javax.sql.DataSource;

import org.postgresql.test.TestUtil;

import com.mchange.v2.c3p0.ComboPooledDataSource;

public class PoolStatementsC3P0DaylightSavingTest extends PoolStatementsDaylightSavingTestBase {

    @Override
    protected DataSource createPooledDataSource() {
        ComboPooledDataSource dataSourceC3PO = new ComboPooledDataSource(true);

        dataSourceC3PO.setJdbcUrl(TestUtil.getURL());
        dataSourceC3PO.setUser(TestUtil.getUser());
        dataSourceC3PO.setPassword(TestUtil.getPassword());

        dataSourceC3PO.setMaxPoolSize(3);

        dataSourceC3PO.setMaxStatements(30); //   <-------------------- this makes the difference  (0 is OK)

        return dataSourceC3PO;

    }

}
