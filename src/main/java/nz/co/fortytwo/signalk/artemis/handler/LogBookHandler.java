package nz.co.fortytwo.signalk.artemis.handler;

import com.fasterxml.jackson.databind.ser.Serializers;
import mjson.Json;
import nz.co.fortytwo.signalk.artemis.util.SignalkKvConvertor;
import nz.co.fortytwo.signalk.artemis.util.Util;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.time.format.DateTimeFormatter;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.NavigableMap;

import static nz.co.fortytwo.signalk.artemis.util.Config.*;
import static nz.co.fortytwo.signalk.artemis.util.SignalKConstants.*;

public class LogBookHandler extends BaseHandler {

    private static Logger logger = LogManager.getLogger(InfluxDbHandler.class);
    private static final int THRESHOLD = 30;  //in minutes
    private static LocalDateTime lastEntryTime = LocalDateTime.now();

    //map to accumulate messages
    private NavigableMap<String, String> entryMap;

    public LogBookHandler() {
        super();
        if (logger.isDebugEnabled())
            logger.debug("Initialising for : {} ", uuid);
        try {
            // start listening
            initSession(
//                    AMQ_INFLUX_KEY+" LIKE '"+vessels+dot+uuid+dot+env_wind_speedApparent+"%' OR "
//                    +AMQ_INFLUX_KEY+" LIKE '"+vessels+dot+uuid+dot+env_wind_angleApparent+"%'OR "
//                    +AMQ_INFLUX_KEY+" LIKE '"+vessels+dot+uuid+dot+nav_speedOverGround+"%'OR "
//                    +AMQ_INFLUX_KEY+" LIKE '"+vessels+dot+uuid+dot+nav_courseOverGroundMagnetic+"%'OR "
//                    +AMQ_INFLUX_KEY+" LIKE '"+vessels+dot+uuid+dot+nav_courseOverGroundTrue+"%' OR "
                    AMQ_INFLUX_KEY+" LIKE '"+vessels+dot+uuid+dot+nav_position+"%'");
        } catch (Exception e) {
            logger.error(e, e);
        }
    }

    public void consume(Message message) {

        String key = message.getStringProperty(AMQ_INFLUX_KEY);
//        if (!AMQ_CONTENT_TYPE_JSON_FULL.equals(message.getStringProperty(AMQ_CONTENT_TYPE)))
//            return;
        Json node = Util.readBodyBuffer( message.toCore());

        // TODO: save all messages of interest (position, gps, sog, stw) that exist
        // TODO: if message of interest -> get last entry timestamp -> check if > threshold -> if true -> save to database
        // if currentTime > lastTimeEntry + 30min -> set lastTimeEntry to currentTime
        if(LocalDateTime.now().isAfter(lastEntryTime.plusMinutes(THRESHOLD))){
            // TODO: save auto entry to database:
            // TODO: generate appropriate key, what is node??
            if (Util.isLogBookFormat(node)) {
                if (logger.isDebugEnabled())
                    logger.debug("processing full {} ", node);
                try {
                    SignalkKvConvertor.parseFull(this,message, node, "");
                    node.clear(true);
                } catch (Exception e) {
                    logger.error(e,e);
                }
            }
            lastEntryTime = LocalDateTime.now();
            //save(key, node)
        }

        // deal with full format

        return;
    }



    protected void save(String key, Json node) {
        influx.save(key, node.dup());
    }

}