package nz.co.fortytwo.signalk.artemis.tdb;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.junit.Assert;
import org.junit.jupiter.api.*;

import java.text.SimpleDateFormat;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

public class InfluxWriteTest {
    private static Logger logger = LogManager.getLogger(InfluxWriteTest.class);
    private static InfluxDB influxDB;
    private static String dbName = "Blackbox";
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SS'Z'");

    @BeforeAll
    public static void setUpTDb() {
        influxDB = InfluxDBFactory.connect("http://localhost:8086", "admin", "admin");
        influxDB.query(new Query("CREATE DATABASE " + dbName));
        influxDB.setDatabase(dbName);

        // write entries into database
        influxDB.write(Point.measurement(dbName)
                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                .addField("key", "wind.angleApparent")
                .addField("value", 4.4336)
                .build());

        long time = System.currentTimeMillis();
        influxDB.write(Point.measurement(dbName)
                .time(time, TimeUnit.MILLISECONDS)
                .addField("key", "wind.angleApparent")
                .addField("value", 4.4336)
                .build());
    }

    @Nested
    @DisplayName("When write Data with timestamp in past")
    class TimeStampInPast {
        long time = System.currentTimeMillis();
        long timeInPast = time - 1000000000;

        @Test
        @DisplayName("Database should have entry with timeStamp in Past")
        void shouldHaveEntryInPast() {
            influxDB.write(Point.measurement(dbName)
                    .time(timeInPast, TimeUnit.MILLISECONDS)
                    .addField("key", "wind.angleApparent")
                    .addField("value", 9.44)
                    .build());
            QueryResult result = influxDB.query(new Query("Select * from " + dbName));
            sdf.setTimeZone(TimeZone.getTimeZone("GMT"));

            Assert.assertEquals(sdf.format(timeInPast), result.getResults().get(0).getSeries().get(0).getValues().get(0).get(0));
            showDatabase();
        }
    }

    @Nested
    @DisplayName("When change data of database entry")
    class ChangeDataOfTimestamp {
        long time = System.currentTimeMillis();

        @Test
        @DisplayName("Entry with given time should have value changed")
        void shouldHaveValueChanged() {
            sdf.setTimeZone(TimeZone.getTimeZone("GMT"));

            influxDB.write(Point.measurement(dbName)
                    .time(time, TimeUnit.MILLISECONDS)
                    .addField("key", "wind.angleApparent")
                    .addField("value", 5.0)
                    .build());
            QueryResult result = influxDB.query(new Query("Select * from " + dbName + " where time = '" + sdf.format(time)+"'"));
            Assert.assertEquals(5.0, result.getResults().get(0).getSeries().get(0).getValues().get(0).get(2));
            showDatabase();


            influxDB.write(Point.measurement(dbName)
                    .time(time, TimeUnit.MILLISECONDS)
                    .addField("key", "wind.angleApparent")
                    .addField("value", 10.0)
                    .build());
            String timeString = sdf.format(time);
            result = influxDB.query(new Query("Select * from " + dbName + " where time = '" + sdf.format(time) +"'"));
            Assert.assertEquals(10.0, result.getResults().get(0).getSeries().get(0).getValues().get(0).get(2));
            showDatabase();

            // should have one entry
            result = influxDB.query(new Query("Select * from " + dbName + " where time = '" + sdf.format(time)+"'"));
            Assert.assertEquals(1, result.getResults().get(0).getSeries().get(0).getValues().size());
        }
    }

    private void showDatabase() {
        QueryResult result = influxDB.query(new Query("Select * from " + dbName));
        result.getResults().get(0).getSeries().get(0).getValues().forEach((el) -> System.out.println(el));
        System.out.println();
    }

    @AfterAll
    static void tearDownAll() {
        influxDB.query(new Query("DROP DATABASE " + dbName));
        influxDB.close();
    }
}
