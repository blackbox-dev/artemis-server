package nz.co.fortytwo.signalk.artemis.tdb;

import mjson.Json;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBException;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import java.text.DecimalFormat;
import java.time.Instant;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class LogbookDbService {

    private InfluxDB logbookInfluxDB;
    private InfluxDbService artemisInfluxDB;
    private static Logger logger = LogManager.getLogger(LogbookDbService.class);
    private String dbName = "logbook";
    private String dbURL = "http://localhost:8086";

    public LogbookDbService() {
        try {
            this.logbookInfluxDB = InfluxDBFactory.connect(dbURL, "admin", "admin");
            // also create an artemisInfluxDB instance in order to query the 'vessels' database
            artemisInfluxDB = new InfluxDbService();
        } catch(InfluxDBException e) {
            logger.error("Failed to connect to influx: ", e);
        }
        /*try {
            // connection successful; set database to logbook
            this.logbookInfluxDB.setDatabase(dbName);
        } catch(InfluxDBException ex) {
            logger.error("Failed to connect to database '" + dbName + "': ", ex);
        }*/
        // connection successful; set database to logbook
        this.logbookInfluxDB.setDatabase(dbName);
        // enable batch writes to get better performance
        logbookInfluxDB.enableBatch(BatchOptions.DEFAULTS);
        // retention policy states how long influx should store the data before deleting it
        logbookInfluxDB.setRetentionPolicy("autogen");
    }

    public void saveToLogbook(String eventType, String timestamp) {
        System.out.println("#####\n#######\n"+timestamp+"\n#####\n#######");
        // Get necessary information from vessels measurement
        String query = "select * from vessels group by skey, primary, uuid, sourceRef order by time desc limit 1";
        NavigableMap<String, Json> map = new ConcurrentSkipListMap<>();
        artemisInfluxDB.loadData(map, query);

        String[] posValues = getValue(map, "navigation.position").split(",");
        String depth = getValue(map, "environment.depth.belowTransducer");
        String aws = getValue(map, "environment.wind.speedApparent");
        String awa = getValue(map, "environment.wind.angleApparent");
        String tws = getValue(map, "environment.wind.speedTrue"); //tws
        String twa = getValue(map, "environment.wind.angleTrueWater");
        String windDirection = getValue(map, "environment.wind.directionTrue");
        String heading = getValue(map, "navigation.headingMagnetic");
        String cog = getValue(map, "navigation.courseOverGroundTrue");
        String stw = getValue(map, "navigation.speedThroughWater");
        String sog = getValue(map, "navigation.speedOverGround");
        String waterTemp = getValue(map, "environment.water.temperature");
        // create a measurement entry
        String test = "1592172537468000000";
        long nano = Instant.parse(timestamp).toEpochMilli();
        String nanoToString = nano + "000000";
        logbookInfluxDB.write(Point.measurement("event")
                //.time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                //.time(Long.parseLong(nano), TimeUnit.NANOSECONDS)
                .time(Long.parseLong(nanoToString), TimeUnit.NANOSECONDS)
                .tag("type", eventType)
                .addField("posLat", Float.parseFloat(posValues[0].trim()))
                .addField("posLong", Float.parseFloat(posValues[1].trim()))
                .addField("heading", Float.parseFloat(heading))
                .addField("COG", Float.parseFloat(cog))
                .addField("STW", Float.parseFloat(stw))
                .addField("SOG", Float.parseFloat(sog))
                .addField("depth", Float.parseFloat(depth))
                .addField("AWS", Float.parseFloat(aws))
                .addField("AWA", Float.parseFloat(awa))
                .addField("TWS", Float.parseFloat(tws))
                .addField("TWA", Float.parseFloat(twa))
                .addField("windDirection", Float.parseFloat(windDirection))
                .addField("waterTemp", Float.parseFloat(waterTemp))
                .build());
        System.out.println("#######\nHERE\n#######");

        // send curl post request to telegraf
        try {
            String line_protocol = String.format("event,type=%s posLat=%s,posLong=%s,heading=%s,STW=%s,SOG=%s,COG=%s,depth=%s,AWS=%s,AWA=%s,TWS=%s,TWA=%s,windDirection=%s,waterTemp=%s %s",
                    eventType, posValues[0].trim(), posValues[1].trim(), heading, stw, sog, cog, depth, aws, awa, tws, twa, windDirection, waterTemp, nanoToString);

            System.out.println("line_protocol: " + line_protocol);
            String[] command = {"/bin/sh", "-c", "curl -i -XPOST 'http://localhost:8186/write' --data-binary '" + line_protocol + "'"};
            Process p = Runtime.getRuntime().exec(command);
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    private String getValue(NavigableMap<String, Json> map, String keyDescription) {
        DecimalFormat df = new DecimalFormat("#.00");
        String retVal = "null";
        //String strKey = df.format(map.keySet().stream().filter(s -> s.endsWith(keyDescription)).collect(Collectors.toSet()).toString());
        String strKey = map.keySet().stream().filter(s -> s.endsWith(keyDescription)).collect(Collectors.toSet()).toString();
        if(strKey.contains("[")) {
            strKey = strKey.replace("[", "").trim();
        }
        if(strKey.contains("]")) {
            strKey = strKey.replace("]", "").trim();
        }

        if (keyDescription == "navigation.position") {
            // create [] with latitude, longitude & altitude
            String latitude = map.get(strKey).at("value").at("latitude").toString();
            String longitude = map.get(strKey).at("value").at("longitude").toString();
            //String altitude = map.get(strKey).at("value").at("longitude").toString(); // currently missing in test set
            retVal = latitude.concat(", ").concat(longitude);
        } else {
            retVal = map.get(strKey).at("value").toString();
        }

        return retVal.trim();
    }

    /**
     * Closes the logbookInfluxDB resource, thus setting underlying resources free.
     */
    public void closeLogbookService() { logbookInfluxDB.close();}

    public List<QueryResult.Series> getMeasurements(String query) {
        //String query = "select * from event";
        //List<QueryResult.Series> queryResult =
        return logbookInfluxDB.query(new Query(query)).getResults().get(0).getSeries();
    }
}