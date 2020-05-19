package nz.co.fortytwo.signalk.artemis.tdb;

import mjson.Json;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBException;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class LogbookDbService {

    private InfluxDB logbookInfluxDB;
    private InfluxDbService artemisInfluxDB;

    public LogbookDbService() {
        try {
            this.logbookInfluxDB = InfluxDBFactory.connect("http://localhost:8086", "admin", "admin");
            artemisInfluxDB = new InfluxDbService();
        } catch(InfluxDBException e) {
            System.out.println("Failed to connect to influx: " + e);
        }
        // successfully connected; save event to logbookDB
        logbookInfluxDB.setDatabase("logbook");
    }


    public void saveToLogbook(String eventType) {
        // Get necessary information from vessels measurement
        String query = "select * from vessels group by skey, primary, uuid, sourceRef order by time desc limit 1";
        NavigableMap<String, Json> map = new ConcurrentSkipListMap<>();
        artemisInfluxDB.loadData(map, query);

        /*for(Map.Entry<String, Json> entry : map.entrySet()) {
            System.out.println("key: " + entry.getKey() + "\nvalue: " + entry.getValue() + "\n" + entry.getValue().at("value"));
            if(entry.getKey().equals("vessels.urn:mrn:signalk:uuid:71c9c917-2d2c-495a-919d-17ab02411f6c.navigation.speedThroughWater")) {
                System.out.println("FOUND KEY");
                StW = entry.getValue().at("value").toString();
            }
        }*/

        /*map.forEach((k, v) -> {
            System.out.println("key: " + k + "value: " + "\n" + v.at("value"));
            if(k.equals("vessels.urn:mrn:signalk:uuid:71c9c917-2d2c-495a-919d-17ab02411f6c.navigation.speedThroughWater")) {
                System.out.println("FOUND KEY");
                //StW = v.at("value").at("value").toString();
            }
        } );*/

        String SoGValue = getValue(map, "navigation.speedOverGround");
        String StWValue = getValue(map, "navigation.speedThroughWater");
        String[] posValues = getValue(map, "navigation.position").split(",");
        // create a measurement entry
        logbookInfluxDB.write(Point.measurement("event")
                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                .addField("eventType", eventType)
                .addField("posLat", posValues[0])
                .addField("posLong", posValues[1])
                .addField("SoG", SoGValue)
                .addField("StW", StWValue)
                .build());
    }

    private String getValue(NavigableMap<String, Json> map, String keyDescription) {
        String retVal = "null";
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
}
