package org.postgresql.test.other;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

import junit.framework.TestCase;

import org.postgresql.test.TestUtil;

public class PreparedStatementDaylightSavingTest extends TestCase {

    private Connection _conn;

    private String dateTimeInSaylightSavingDay;

    /** Hours that will produce this problem 3AM, 4AM, 5AM and 6AM */
    private int testHour = 6;

    private String uniqueTestName;

    @Override
    protected void setUp() throws Exception {
        _conn = TestUtil.openDB();
        TestUtil.createTable(_conn, "dls1test", "name varchar(500), val timestamp");

        // daylight saving time in EDT  2014-03-09  from 2:00 jumps to -> 3:00
        //http://www.timetemperature.com/canada/daylight_saving_time_canada.shtml
        dateTimeInSaylightSavingDay = "2014-03-09 06:21:28";
        testHour = 6;

        // Other dates with the same problem
        //dateTimeInSaylightSavingDay = "2009-03-08 06:21:28";
        //dateTimeInSaylightSavingDay = "2010-03-14 06:21:28";
        //dateTimeInSaylightSavingDay = "2015-03-08 06:21:28";

        //Setup test data
        uniqueTestName = "Test#" + System.nanoTime();

        // Save event in day when daylight saving change happened 
        {
            PreparedStatement stmt = _conn.prepareStatement("INSERT INTO dls1test (name, val)  VALUES (?, ?)");

            stmt.setString(1, uniqueTestName);
            Date date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH).parse(dateTimeInSaylightSavingDay);
            stmt.setTimestamp(2, new java.sql.Timestamp(date.getTime()));

            stmt.executeUpdate();
            stmt.close();

        }
    }

    @Override
    protected void tearDown() throws SQLException {
        TestUtil.dropTable(_conn, "dls1test");
        TestUtil.closeDB(_conn);
    }

    public void testDateTimeInSaylightSavingDayFromTable() throws SQLException {
        String sql = "SELECT val FROM dls1test WHERE name = ?";
        PreparedStatement stmt = _conn.prepareStatement(sql);

        for (int i = 1; i <= 20; i++) {
            stmt.setString(1, uniqueTestName);

            ResultSet rs = stmt.executeQuery();
            try {
                if (rs.next()) {

                    java.sql.Timestamp value;

                    value = rs.getTimestamp("val");
                    //value = rs.getTimestamp("val", Calendar.getInstance()); // <-- this changes nothing

                    Calendar c = Calendar.getInstance();
                    c.setTime(value);

                    //System.out.println(value.getTimezoneOffset()); // this prints the same value

                    assertEquals("try# " + i + " to verify hour of " + value, testHour, c.get(Calendar.HOUR_OF_DAY));

                } else {
                    throw new Error();
                }
            } finally {
                rs.close();
            }

        }
    }
}
