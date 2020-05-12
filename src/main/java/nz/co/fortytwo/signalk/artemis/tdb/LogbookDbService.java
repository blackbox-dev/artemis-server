package nz.co.fortytwo.signalk.artemis.tdb;

import mjson.Json;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBException;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;

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
        // successfully connection. Save event to logbookDB
        logbookInfluxDB.setDatabase("logbook");
    }

    public void saveToLogbook(String event) {
        // Get necessary information from vessels measurement
        String query = "select * from vessels group by skey, primary, uuid, sourceRef order by time desc limit 1";
        NavigableMap<String, Json> map = new ConcurrentSkipListMap<>();
        artemisInfluxDB.loadData(map, query);
        map.forEach((k, v) -> {
            System.out.println("key: " + k + "value: " + "\n" + v.at("value"));
            if(k.contains("navigation.positionvalue:")) {
                System.out.println("FOUND KEY");
            }
        } );
        String SoG = map.get("vessels.urn:mrn:signalk:uuid:71c9c917-2d2c-495a-919d-17ab02411f6c.navigation.speedOverGround:").toString();
        // create a measurement entry
        logbookInfluxDB.write(Point.measurement("event")
            .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
            .addField("eventType", event)
            .addField("position", "getPos1")
            .addField("SoG", "m/s")
            .addField("StW", "m/s")
            .build());
    }

    /**
     * Closes the logbookInfluxDB resource, thus setting underlying resources free.
     */
    public void closeLogbookService() { logbookInfluxDB.close();}
}
