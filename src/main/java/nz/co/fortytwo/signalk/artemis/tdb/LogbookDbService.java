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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
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

    public LogbookDbService() {
        try {
            this.logbookInfluxDB = InfluxDBFactory.connect("http://localhost:8086", "admin", "admin");
            // also create an artemisInfluxDB instance in order to query the 'vessels' database
            artemisInfluxDB = new InfluxDbService();
        } catch(InfluxDBException e) {
            logger.error("Failed to connect to influx: ", e);
        }
        // connection successful; set database to logbook
        logbookInfluxDB.setDatabase(dbName);
        // enable batch writes to get better performance
        logbookInfluxDB.enableBatch(BatchOptions.DEFAULTS);
        // retention policy states how long influx should store the data before deleting it
        logbookInfluxDB.setRetentionPolicy("autogen");
    }

    public void saveToLogbook(String eventType) {
        // Get necessary information from vessels measurement
        String query = "select * from vessels group by skey, primary, uuid, sourceRef order by time desc limit 1";
        NavigableMap<String, Json> map = new ConcurrentSkipListMap<>();
        artemisInfluxDB.loadData(map, query);

        /*
        * Important values are time, position, heading (navigation.headingMagnetic?), COG (true or magnetic?)
        * STW, SOG, Depth, True wind speed(environment.wind.speetTrue), true wind direction
        * water temperature, air temperature
        * */
        String[] posValues = getValue(map, "navigation.position").split(",");
        String depth = getValue(map, "environment.depth.belowTransducer");
        String windSpeed = getValue(map, "environment.wind.speedTrue");
        String windDirection = getValue(map, "environment.wind.directionTrue");
        String heading = getValue(map, "navigation.headingMagnetic");
        String cog = getValue(map, "navigation.courseOverGroundTrue");
        String stw = getValue(map, "navigation.speedThroughWater");
        String sog = getValue(map, "navigation.speedOverGround");
        String waterTemp = getValue(map, "environment.water.temperature");
        // create a measurement entry
        logbookInfluxDB.write(Point.measurement("event")
                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                .addField("eventType", eventType)
                .addField("posLat", posValues[0].trim())
                .addField("posLong", posValues[1].trim())
                .addField("depth", depth)
                .addField("windSpeed", windSpeed)
                .addField("windDirection", windDirection)
                .addField("heading", heading)
                .addField("cog", cog)
                .addField("StW", stw)
                .addField("SoG", sog)
                .addField("waterTemp", waterTemp)
                .build());

        // send curl post request to telegraf
        try {
            String line_protocol = String.format("event,eventType=%s posLat=%s,posLong=%s,depth=%s,windSpeed=%s,windDirection=%s,heading=%s,cog=%s,stw=%s,SoG=%s,waterTemp=%s",
                    eventType, posValues[0].trim(), posValues[1].trim(), depth, windSpeed, windDirection, heading, cog, stw, sog, waterTemp);
            System.out.println("line_protocol: " + line_protocol);
            String[] command = {"/bin/sh", "-c", "curl -i -XPOST 'http://localhost:8186/write' --data-binary '" + line_protocol + "'"};
            Process p = Runtime.getRuntime().exec(command);
            //s
            // p.destroy();
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

    public List<QueryResult.Series> getMeasurements() {
        String query = "select * from event";
        //List<QueryResult.Series> queryResult =
        return logbookInfluxDB.query(new Query(query)).getResults().get(0).getSeries();
    }
}
