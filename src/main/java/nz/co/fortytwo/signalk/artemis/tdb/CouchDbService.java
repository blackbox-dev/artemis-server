package nz.co.fortytwo.signalk.artemis.tdb;

import com.google.gson.JsonObject;
import mjson.Json;
import nz.co.fortytwo.signalk.artemis.util.Config;
import nz.co.fortytwo.signalk.artemis.util.ConfigConstants;
import nz.co.fortytwo.signalk.artemis.util.Util;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.influxdb.dto.Point;
import org.lightcouch.CouchDbClient;
import org.lightcouch.CouchDbProperties;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;

import static nz.co.fortytwo.signalk.artemis.util.SignalKConstants.*;
import static nz.co.fortytwo.signalk.artemis.util.SignalKConstants.timestamp;

public class CouchDbService implements TDBService {
    private static final String STR_VALUE = "strValue";
    private static final String LONG_VALUE = "longValue";
    private static final String DOUBLE_VALUE = "doubleValue";
    private static final String NULL_VALUE = "nullValue";
    public static final String PRIMARY_VALUE = "primary";
    public static boolean allowWrite=false;
    private static Logger logger = LogManager.getLogger(InfluxDbService.class);
    public static final ConcurrentSkipListMap<String, String> primaryMap = new ConcurrentSkipListMap<>();

    CouchDbClient couchDbClient;

    @Override
    public void setUpTDb() {
        CouchDbProperties properties = new CouchDbProperties()
                .setDbName("couchBlackbox")
                .setCreateDbIfNotExist(true)
                .setProtocol("http")
                .setHost("127.0.0.1")
                .setPort(5984)
                .setUsername("admin")
                .setPassword("admin")
                .setMaxConnections(100)
                .setConnectionTimeout(0);

        couchDbClient = new CouchDbClient(properties);
    }

    @Override
    public void setWrite(boolean write) {
        logger.info("Set write: {}", write);
        allowWrite=write;
    }

    @Override
    public boolean getWrite() {
        return false;
    }

    @Override
    public void closeTDb() { couchDbClient.shutdown(); }

    //TODO
    @Override
    public NavigableMap<String, Json> loadResources(NavigableMap<String, Json> map, Map<String, String> query) {
        return null;
    }

    //TODO
    @Override
    public NavigableMap<String, Json> loadConfig(NavigableMap<String, Json> map, Map<String, String> query) {
        return null;
    }

    //TODO
    @Override
    public NavigableMap<String, Json> loadData(NavigableMap<String, Json> map, String table, Map<String, String> query) {
        return null;
    }

    //TODO
    @Override
    public NavigableMap<String, Json> loadSources(NavigableMap<String, Json> map, Map<String, String> query) {
        return null;
    }

    @Override
    public void save(NavigableMap<String, Json> map) {
        if (logger.isDebugEnabled())logger.debug("Save map:  {}" ,map);
        for(Map.Entry<String, Json> e: map.entrySet()){
            save(e.getKey(), e.getValue());
        }
        // TODO: influxDB.flush(); -> what does it do & find same for Couchdb
    }

    // TODO: !duplicate method like in InfluxDbService!
    @Override
    public void save(String k, Json v) {
        if (logger.isDebugEnabled())logger.debug("Save json:  {}={}" , k , v);

        if(k.contains("._attr")){
            return;
        }
        if(k.contains("jwtToken")){
            return;
        }
        String srcRef = null;
        if(v.isObject() && v.has(sourceRef)) {
            srcRef=v.at(sourceRef).asString();
        }else {
            srcRef= StringUtils.substringAfter(k,dot+values+dot);
            if(srcRef.contains(dot+meta+dot))srcRef=StringUtils.substringBefore(srcRef, dot+meta);
        }
        if(StringUtils.isBlank(srcRef))srcRef="self";
        long tStamp = (v.isObject() && v.has(timestamp) ? Util.getMillisFromIsoTime(v.at(timestamp).asString())
                : System.currentTimeMillis());
        save(k,v,srcRef, tStamp);
    }

    // TODO: !duplicate method like in InfluxDbService!
    @Override
    public void save(String k, Json v, String srcRef, long tStamp) {
        if (v.isPrimitive()|| v.isBoolean()) {

            if (logger.isDebugEnabled())logger.debug("Save primitive:  {}={}", k, v);
            saveData(k, srcRef, tStamp, v.getValue());
            return;
        }
        if (v.isNull()) {

            if (logger.isDebugEnabled())logger.debug("Save null: {}={}",k , v);
            saveData(k, srcRef, tStamp, null);
            return;
        }
        if (v.isArray()) {

            if (logger.isDebugEnabled())logger.debug("Save array: {}={}", k , v);
            saveData(k, srcRef, tStamp, v);
            return;
        }
        if (v.has(sentence)) {
            saveData(k + dot + sentence, srcRef, tStamp, v.at(sentence));
        }
        if (v.has(meta)) {
            for (Map.Entry<String, Json> i : v.at(meta).asJsonMap().entrySet()) {

                if (logger.isDebugEnabled())logger.debug("Save meta: {}={}",()->i.getKey(), ()->i.getValue());
                saveData(k + dot + meta + dot + i.getKey(), srcRef, tStamp, i.getValue());
            }
        }

        if (v.has(values)) {

            for (Map.Entry<String, Json> i : v.at(values).asJsonMap().entrySet()) {

                if (logger.isDebugEnabled())logger.debug("Save values: {}={}",()->i.getKey() ,()-> i.getValue());
                String sRef = StringUtils.substringBefore(i.getKey(),dot+value);
                Json vs = i.getValue();
                long ts = (vs.isObject() && vs.has(timestamp) ? Util.getMillisFromIsoTime(vs.at(timestamp).asString())
                        : tStamp);
                save(k,i.getValue(),sRef, ts);

            }
        }

        if (v.has(value)&& v.at(value).isObject()) {
            for (Map.Entry<String, Json> i : v.at(value).asJsonMap().entrySet()) {

                if (logger.isDebugEnabled())logger.debug("Save value object: {}={}" , ()->i.getKey(),()->i.getValue());
                saveData(k + dot + value + dot + i.getKey(), srcRef, tStamp, i.getValue());
            }
            return;
        }


        if (logger.isDebugEnabled())logger.debug("Save value: {} : {}", k, v);
        saveData(k + dot + value, srcRef, tStamp, v.at(value));

        return;
    }

    //TODO: refactor first part of method into new one
    protected void saveData(String key, String sourceRef, long timeMillis, Object val) {
        if(!allowWrite) {
            String clock = null;
            try {
                clock = Config.getConfigProperty(ConfigConstants.CLOCK_SOURCE);
            }catch (Exception e) {
                //ignore
            }
            if(StringUtils.equals(clock,"system")) {
                //if (logger.isInfoEnabled())logger.info("write enabled for {} : {}",()->val.getClass().getSimpleName(),()->key);
                setWrite(true);
            }else {
                if (logger.isInfoEnabled())logger.info("write not enabled for {} : {}",()->val.getClass().getSimpleName(),()->key);
                return;
            }
        }
        if(val!=null)
            if (logger.isDebugEnabled())logger.debug("save {} : {}",()->val.getClass().getSimpleName(),()->key);
            else{
                if (logger.isDebugEnabled())logger.debug("save {} : {}",()->null,()->key);
            }

        if(self_str.equals(key))return;
        if(version.equals(key))return;
        //String[] path = StringUtils.split(key, '.');
        //StringUtils.substringBetween(key, dot, dot)
        try {
            int p1 = key.indexOf(dot);
            if(p1<0) {
                p1=key.length();
            }
            int p2 = key.indexOf(dot,p1+1);
            int p3 = p2+1;
            if(p2<0) {
                p2=key.length();
                p3=p2;
            }



            JsonObject json = new JsonObject();
            // TODO:
            // database to use key.substring(0, p1))
            json.addProperty("time", timeMillis);
            switch (key.substring(0, p1)) {
                case resources:
                    json.addProperty("sourceRef", sourceRef);
                    json.addProperty("uuid", key.substring(p1+1, p2));
                    json.addProperty(PRIMARY_VALUE, isPrimary(key,sourceRef).toString());
                    json.addProperty(skey, key.substring(p3));
                    break;
                case sources:
                    json.addProperty("sourceRef", key.substring(p1+1, p2));
                    json.addProperty(skey, key.substring(p3));
                    break;
                case CONFIG:
                    json.addProperty(skey, key.substring(p1+1));
                    break;
                case vessels:
                case aircraft:
                case sar:
                case aton:
                    Boolean primary = isPrimary(key,sourceRef);
                    json.addProperty("sourceRef", sourceRef);
                    json.addProperty("uuid", key.substring(p1+1, p2));
                    json.addProperty(PRIMARY_VALUE, primary.toString());
                    json.addProperty(skey, key.substring(p3));
                    break;
                default:
                    break;
            }
            addFieldValue(json, val);
            couchDbClient.save(json);

        }catch (Exception e) {
            logger.error(" Failed on key {} : {}",key, e.getMessage(), e);
            throw e;
        }
    }

    private void addFieldValue(JsonObject jsonObject, Object fieldValue) {
        Object value = null;

        if(fieldValue==null) {
            jsonObject.addProperty(NULL_VALUE, true);
        }
        if(fieldValue instanceof Json){
            if(((Json)fieldValue).isNull()){
                jsonObject.addProperty(NULL_VALUE, true);
            }else if(((Json)fieldValue).isString()){
                value=((Json)fieldValue).asString();
            }else if(((Json)fieldValue).isArray()){
                value=((Json)fieldValue).toString();
            }else if(((Json)fieldValue).isObject()){
                value=((Json)fieldValue).toString();
            }else{
                value=((Json)fieldValue).getValue();
            }
        }else{
            value=fieldValue;
        }

        if(value instanceof Boolean) jsonObject.addProperty(STR_VALUE, ((Boolean)value).toString());
        else if(value instanceof Double) jsonObject.addProperty(DOUBLE_VALUE, (Double)value);
        else if(value instanceof Float) jsonObject.addProperty(DOUBLE_VALUE, (Double)value);
        else if(value instanceof BigDecimal) jsonObject.addProperty(DOUBLE_VALUE, ((BigDecimal)value).doubleValue());
        else if(value instanceof Long) jsonObject.addProperty(LONG_VALUE, (Long)value);
        else if(value instanceof Integer) jsonObject.addProperty(LONG_VALUE, ((Integer)value).longValue());
        else if(value instanceof BigInteger) jsonObject.addProperty(LONG_VALUE, ((BigInteger)value).longValue());
        else if(value instanceof String) jsonObject.addProperty(STR_VALUE, (String)value);

        if (logger.isDebugEnabled())logger.debug("add FieldValue",value);
    }

    @Override
    public Boolean isPrimary(String key, String sourceRef) {
        //truncate the .values. part of the key
        String mapRef = primaryMap.get(StringUtils.substringBefore(key,dot+values+dot));

        boolean bool = false;
        if(mapRef==null){
            setPrimary(key, sourceRef);
            bool = true;
        } else if(StringUtils.equals(sourceRef, mapRef)){
            bool = true;
        }
        if (logger.isDebugEnabled())logger.debug("isPrimary: {}={} : {}, {}",key,sourceRef, mapRef, false);
        return bool;
    }

    // TODO
    @Override
    public void loadPrimary() {

    }

    @Override
    public Boolean setPrimary(String key, String sourceRef) {
        if (logger.isDebugEnabled())logger.debug("setPrimary: {}={}",key,sourceRef);
        return !StringUtils.equals(sourceRef, primaryMap.put(StringUtils.substringBefore(key,dot+values+dot),sourceRef));
    }

    @Override
    public void close() {
        couchDbClient.shutdown();
    }

    // TODO
    @Override
    public NavigableMap<String, Json> loadDataSnapshot(NavigableMap<String, Json> map, String table, Map<String, String> query, String time) {
        return null;
    }

    // TODO
    @Override
    public NavigableMap<String, Json> loadDataSnapshot(NavigableMap<String, Json> rslt, String table, Map<String, String> map, long queryTime) {
        return null;
    }

    // TODO
    @Override
    public void clearDbFuture() {

    }

    // TODO
    @Override
    public void clearDbVessels() {

    }
}
