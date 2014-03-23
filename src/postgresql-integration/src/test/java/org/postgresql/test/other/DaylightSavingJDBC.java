package org.postgresql.test.other;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.impl.GenericObjectPool;

import com.mchange.v2.c3p0.ComboPooledDataSource;

public class DaylightSavingJDBC {

    static String dateTimeInSaylightSavingDay;

    /** Hours that will produce this problem 3AM, 4AM, 5AM and 6AM */
    static int testHour = 6;

    static {
        // daylight saving time in EDT  2014-03-09  from 2:00 jumps to -> 3:00
        //http://www.timetemperature.com/canada/daylight_saving_time_canada.shtml
        dateTimeInSaylightSavingDay = "2014-03-09 06:21:28";

        // Other dates with the same problem
        //dateTimeInSaylightSavingDay = "2009-03-08 06:21:28";
        //dateTimeInSaylightSavingDay = "2010-03-14 06:21:28";
        //dateTimeInSaylightSavingDay = "2015-03-08 06:21:28";
    }

    enum DBType {
        HSQL, MySQL, PostgreSql, Oracle
    }

    public enum ConnectionPoolProvider {
        none, dbcp, c3p0
    };

    static boolean init = true;

    static boolean firstRun = true;

    public static void main(String[] args) throws Exception {
        List<String> arguments = Arrays.asList(args);

        Connection con;

        DBType testDB;

        //testDB = DBType.HSQL;
        //testDB = DBType.MySQL;
        testDB = DBType.PostgreSql;

        if (arguments.contains("-PostgreSql")) {
            testDB = DBType.PostgreSql;
        } else if (arguments.contains("-MySQL")) {
            testDB = DBType.MySQL;
        } else if (arguments.contains("-HSQL")) {
            testDB = DBType.HSQL;
        }

        ConnectionPoolProvider connectionPoolProvider;

        //connectionPoolProvider = ConnectionPoolProvider.c3p0;
        connectionPoolProvider = ConnectionPoolProvider.none;

        if (arguments.contains("-c3p0")) {
            connectionPoolProvider = ConnectionPoolProvider.c3p0;
        } else if (arguments.contains("-dbcp")) {
            connectionPoolProvider = ConnectionPoolProvider.dbcp;
        } else if (arguments.contains("-none")) {
            connectionPoolProvider = ConnectionPoolProvider.none;
        }

        System.out.println("DB type: " + testDB + " ConnectionPool:" + connectionPoolProvider);
        System.out.println(System.getProperty("java.version") + " " + System.getProperty("java.vm.name"));

        String driverClass;
        String url;
        String user;
        String password;

        switch (testDB) {
        case HSQL: {
            driverClass = "org.hsqldb.jdbcDriver";
            Class.forName(driverClass);
            url = "jdbc:hsqldb:mem:tst_entity";
            user = "sa";
            password = "";
            con = DriverManager.getConnection(url, user, password);
            // Create test Table
            if (init) {
                init = false;
                con.createStatement()
                        .executeUpdate(
                                "CREATE TABLE test(id bigint generated by default as identity (start with 1),  name varchar(500), dval timestamp,  CONSTRAINT TEST_PK PRIMARY KEY (id))");
            }
            break;
        }
        case MySQL: {
            driverClass = "com.mysql.jdbc.Driver";
            Class.forName(driverClass).newInstance();

            url = "jdbc:mysql://localhost/tst_entity";
            user = "tst_entity";
            password = "tst_entity";
            con = DriverManager.getConnection(url, user, password);

            if (init) {
                init = false;
                con.createStatement().executeUpdate(
                        "CREATE TABLE test(id bigint(20) NOT NULL AUTO_INCREMENT, name varchar(500), dval datetime,  CONSTRAINT TEST_PK PRIMARY KEY (id))");
            } else {
                con.createStatement().executeUpdate("DELETE FROM test");
            }
            break;
        }
        case PostgreSql: {
            driverClass = "org.postgresql.Driver";
            Class.forName(driverClass).newInstance();

            url = "jdbc:postgresql://localhost/tst_entity";
            user = "tst_entity";
            password = "tst_entity";
            con = DriverManager.getConnection(url, user, password);

            if (init) {
                init = false;
                try {
                    con.createStatement().executeUpdate(
                            "CREATE TABLE test2(id SERIAL NOT NULL, name varchar(500), trc varchar(500), dval timestamp, CONSTRAINT TEST_PK PRIMARY KEY (id))");
                } catch (SQLException e) {
                    if (!e.getMessage().contains("already exists")) {
                        throw e;
                    }
                }
            } else {
                con.createStatement().executeUpdate("DELETE FROM test2");
            }
            break;
        }
        default:
            throw new IllegalArgumentException();
        }

        con.setAutoCommit(false);
        con.createStatement();

        //Setup test data
        String uniqueName = "TestDate#" + System.nanoTime();

        // Save event in day when daylight saving change happened 
        {
            PreparedStatement stmt = con.prepareStatement("INSERT INTO test2 (name, dval)  VALUES (?, ?)");

            stmt.setString(1, uniqueName);
            Date date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH).parse(dateTimeInSaylightSavingDay);
            stmt.setTimestamp(2, new java.sql.Timestamp(date.getTime()));

            stmt.executeUpdate();

        }

        con.commit();

        ComboPooledDataSource dataSourceC3PO = new ComboPooledDataSource(true);
        {
            dataSourceC3PO.setDriverClass(driverClass);
            dataSourceC3PO.setJdbcUrl(url);
            dataSourceC3PO.setUser(user);
            dataSourceC3PO.setPassword(password);

            dataSourceC3PO.setMaxPoolSize(5);

            dataSourceC3PO.setMaxStatements(200); //   <-------------------- this makes the difference  (0 is OK)
        }

        DataSource dataSourceDBCP;
        GenericObjectPool<PoolableConnection> connectionPool;
        {
            ConnectionFactory driverConnectionFactory = new DriverManagerConnectionFactory(url, user, password);

            PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(driverConnectionFactory, null);

            poolableConnectionFactory.setPoolStatements(true); //   <-------------------- this makes the difference  (false is OK)
            poolableConnectionFactory.setMaxOpenPrepatedStatements(30);

            connectionPool = new GenericObjectPool<PoolableConnection>(poolableConnectionFactory);
            connectionPool.setMaxTotal(3);

            poolableConnectionFactory.setPool(connectionPool);
            dataSourceDBCP = new PoolingDataSource<PoolableConnection>(connectionPool);
        }

        PreparedStatement reuseStmt = null;

        try {
            for (int i = 1; i <= 21; i++) {

                Connection con2 = null;
                boolean reuse = false;

                switch (connectionPoolProvider) {
                case none:
                    con2 = con; // <-------------  Not pooled connection works fine!
                    reuse = true;

                    //con2 = DriverManager.getConnection(url, user, password); // <-------------  Driver Managed connection works fine!
                    break;
                case c3p0:
                    con2 = dataSourceC3PO.getConnection(); // <-- this will create error
                    break;
                case dbcp:
                    con2 = dataSourceDBCP.getConnection(); // <-- this will create error
                    break;
                }

                String sql = "SELECT dval FROM test2  WHERE name = ? and trc is null or trc = ?";
                PreparedStatement stmt;
                if (reuse && (reuseStmt != null)) {
                    stmt = reuseStmt;
                } else {
                    stmt = con2.prepareStatement(sql);
                }
                reuseStmt = stmt;
                stmt.setString(1, uniqueName);
                stmt.setString(2, String.valueOf(i));

                ResultSet rs = stmt.executeQuery();
                try {
                    if (rs.next()) {

                        java.sql.Timestamp value;

                        value = rs.getTimestamp("dval");
                        //value = rs.getTimestamp("dval", Calendar.getInstance()); // <-- this changes nothing

                        Calendar c = Calendar.getInstance();
                        c.setTime(value);

                        //System.out.println(value.getTimezoneOffset()); // this prints the same value

                        if (testHour != c.get(Calendar.HOUR_OF_DAY)) {
                            throw new Error("try# " + i + "; hour of " + value + " expected " + testHour + " but was " + c.get(Calendar.HOUR_OF_DAY));
                        }
                    } else {
                        throw new Error();
                    }
                } finally {
                    rs.close();
                    if (!reuse) {
                        stmt.close();
                    }
                }

                if (con2 != con) { // if pooled connection
                    con2.close();
                }
            }
        } finally {
            dataSourceC3PO.close();
            connectionPool.close();
            con.close();
        }

        if (firstRun) {
            System.out.println("Ok :) no problem with date: " + dateTimeInSaylightSavingDay);
            firstRun = false;
        }

    }
}
