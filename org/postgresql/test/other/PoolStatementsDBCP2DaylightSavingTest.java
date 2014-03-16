/*
 * Pyx4j framework
 * Copyright (C) 2008-2013 pyx4j.com.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * Created on Mar 15, 2014
 * @author vlads
 * @version $Id: code-templates.xml 12647 2013-05-01 18:01:19Z vlads $
 */
package org.postgresql.test.other;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.postgresql.test.TestUtil;

public class PoolStatementsDBCP2DaylightSavingTest extends PoolStatementsDaylightSavingTestBase {

    @Override
    protected DataSource createPooledDataSource() {
        GenericObjectPool<PoolableConnection> connectionPool;

        ConnectionFactory driverConnectionFactory = new DriverManagerConnectionFactory(TestUtil.getURL(), TestUtil.getUser(), TestUtil.getPassword());

        PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(driverConnectionFactory, null);

        poolableConnectionFactory.setPoolStatements(true); //   <-------------------- this makes the difference  (false is OK)
        poolableConnectionFactory.setMaxOpenPrepatedStatements(30);

        connectionPool = new GenericObjectPool<>(poolableConnectionFactory);
        connectionPool.setMaxTotal(3);

        poolableConnectionFactory.setPool(connectionPool);

        return new PoolingDataSource<PoolableConnection>(connectionPool);

    }
}
