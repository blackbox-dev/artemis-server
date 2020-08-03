package nz.co.fortytwo.signalk.artemis.util;

import static nz.co.fortytwo.signalk.artemis.util.ConfigConstants.CLOCK_SOURCE;
import static nz.co.fortytwo.signalk.artemis.util.SignalKConstants.dot;
import static nz.co.fortytwo.signalk.artemis.util.SignalKConstants.self_str;
import static nz.co.fortytwo.signalk.artemis.util.SignalKConstants.timestamp;
import static nz.co.fortytwo.signalk.artemis.util.SignalKConstants.uuid;
import static nz.co.fortytwo.signalk.artemis.util.SignalKConstants.vessels;

import java.io.IOException;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import mjson.Json;
import nz.co.fortytwo.signalk.artemis.tdb.InfluxDbService;
import nz.co.fortytwo.signalk.artemis.tdb.TDBService;

/**
 * Listen to the config.* queues and cache the results for easy access.
 * 
 * @author robert
 *
 */
public class Config {

	public static final String ADMIN_USER = "config.server.admin.user";
	public static final String ADMIN_PWD = "config.server.admin.password";

	private static Logger logger = LogManager.getLogger(Config.class);

//	private static ConfigListener listener;
	private static NavigableMap<String, Json> map = new ConcurrentSkipListMap<>();
	private static TDBService influx = new InfluxDbService();
	
	//private static Config config = null;
	private static NavigableMap<String, Json> defaultMap = new ConcurrentSkipListMap<>();

	static {
		try {
			Config.setDefaults(defaultMap);
			Config.setDefaults(map);
			map = Config.loadConfig(map);
			//config = new Config();
			if(StringUtils.equals(map.get(CLOCK_SOURCE).asString(), "system")){
				influx.setWrite(true);
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	public static final String _AMQ_LVQ_NAME = "_AMQ_LVQ_NAME";
	public static final String AMQ_CONTENT_TYPE = "AMQ_content_type";
	public static final String AMQ_SESSION_ID = "AMQ_session_id";
	public static final String AMQ_USER_TOKEN = "AMQ_USER_TOKEN";
	public static final String AMQ_USER_ROLES = "AMQ_USER_ROLES";
	public static final String AMQ_CONTENT_TYPE_JSON = "JSON";
	public static final String AMQ_CONTENT_TYPE_JSON_FULL = "JSON_FULL";
	public static final String AMQ_CONTENT_TYPE_JSON_DELTA = "JSON_DELTA";
	public static final String AMQ_CONTENT_TYPE_JSON_GET = "JSON_GET";
	public static final String AMQ_CONTENT_TYPE_JSON_AUTH = "JSON_AUTH";
	//public static final String JSON_MAP = "JSON_MAP";
	public static final String AMQ_CONTENT_TYPE_JSON_SUBSCRIBE = "JSON_SUBSCRIBE";
	public static final String AMQ_CONTENT_TYPE__0183 = "0183";
	public static final String AMQ_CONTENT_TYPE_STOMP = "STOMP";
	public static final String AMQ_CONTENT_TYPE_AIS = "AIS";
	public static final String AMQ_CONTENT_TYPE_N2K = "N2K";
	public static final String AMQ_CONTENT_TYPE_MQTT = "MQTT";
	
	public static final String AMQ_CORR_ID = "AMQ_CORR_ID";
	public static final String AMQ_REPLY_Q = "AMQ_REPLY_Q";
	public static final String JAVA_TYPE = "JAVA_TYPE";
	public static final String SK_TYPE = "SK_TYPE";
	public static final String SK_TYPE_COMPOSITE = "SK_COMPOSITE";
	public static final String SK_TYPE_VALUE = "SK_VALUE";
	public static final String SK_TYPE_ATTRIBUTE = "SK_ATTRIBUTE";

	public static final String MSG_SRC_IP = "MSG_SRC_IP";
	public static final String MSG_SRC_BUS = "MSG_SRC_BUS";
	public static final String MSG_SRC_TYPE = "MSG_SRC_TYPE";
	public static final String MSG_SRC_TYPE_INTERNAL_PROCESS = "INTERNAL_PROCESS";
	public static final String MSG_SRC_TYPE_INTERNAL_IP = "INTERNAL_IP";
	public static final String MSG_SRC_TYPE_EXTERNAL_IP = "EXTERNAL_IP";
	public static final String MSG_SRC_TYPE_SERIAL = "SERIAL";
	public static final String AMQ_SUB_DESTINATION = "AMQ_SUB_DESTINATION";
	public static final String AMQ_INFLUX_KEY="AMQ_INFLUX_KEY";

	public static final String INCOMING_RAW = "incoming.raw";
	public static final String INTERNAL_KV = "internal.kv";
	public static final String INTERNAL_DISCARD = "internal.discard";
	public static final String OUTGOING_REPLY = "outgoing.reply";

	protected Config() {
//		listener = new ConfigListener(map, (String) map.get(ADMIN_USER).asString(),
//				(String) map.get(ADMIN_PWD).asString());
	}

//	public static Config getInstance() {
//		return config;
//	}

	private static Map<String, Json> getMap() {
		return map;
	}

	public static void setProperty(String property, Json value){
		map.put(property, value);
	}
	public static Json getDiscoveryMsg(String hostname) {

		Json version = Json.object();
		String ver = getConfigProperty(ConfigConstants.VERSION);
		ver = StringUtils.removeStart(ver,"v");
		version.set("version", ver);
		boolean ssl = getConfigPropertyBoolean(ConfigConstants.SECURITY_SSL_ENABLE);
		String ws = ssl ?"wss":"ws";
		String http = ssl?"https":"http";
		int restPort = ssl?getConfigPropertyInt(ConfigConstants.REST_PORT_SSL):getConfigPropertyInt(ConfigConstants.REST_PORT);
		int wsPort = ssl?getConfigPropertyInt(ConfigConstants.WEBSOCKET_PORT_SSL):getConfigPropertyInt(ConfigConstants.WEBSOCKET_PORT);
		
		version.set(SignalKConstants.websocketUrl, ws+"://" + hostname + ":"
				+ wsPort + SignalKConstants.SIGNALK_WS);
		version.set(SignalKConstants.restUrl, http + "://" + hostname + ":"
				+ restPort + SignalKConstants.SIGNALK_API + "/");
		version.set(SignalKConstants.signalkTcpPort,
				"tcp://" + hostname + ":" + getConfigPropertyInt(ConfigConstants.TCP_PORT));
		version.set(SignalKConstants.signalkUdpPort,
				"udp://" + hostname + ":" + getConfigPropertyInt(ConfigConstants.UDP_PORT));
		version.set(SignalKConstants.nmeaTcpPort,
				"tcp://" + hostname + ":" + getConfigPropertyInt(ConfigConstants.TCP_NMEA_PORT));
		version.set(SignalKConstants.nmeaUdpPort,
				"udp://" + hostname + ":" + getConfigPropertyInt(ConfigConstants.UDP_NMEA_PORT));
		if (getConfigPropertyBoolean(ConfigConstants.START_STOMP))
			version.set(SignalKConstants.stompPort,
					"stomp+nio://" + hostname + ":" + getConfigPropertyInt(ConfigConstants.STOMP_PORT));
		if (getConfigPropertyBoolean(ConfigConstants.START_MQTT))
			version.set(SignalKConstants.mqttPort,
					"mqtt://" + hostname + ":" + getConfigPropertyInt(ConfigConstants.MQTT_PORT));
		Json endpoints = Json.object();
		endpoints.set("v" + ver.substring(0, 1), version);
		Json server = Json.object("id","artemis-java-server", "version", ver);
		
		Json reply = Json.object().set("endpoints", endpoints);
		reply.set("server",server);
		return reply;
	}
	
	public static Json getHelloMsg() {

		Json reply = Json.object();
		reply.set("name", "signalk-server");
		reply.set("roles", "[\"master\",\"main\"]");
		String ver = getConfigProperty(ConfigConstants.VERSION);
		ver = StringUtils.removeStart(ver,"v");
		reply.set("version", ver);
		reply.set(timestamp, Util.getIsoTimeString());
		reply.set(self_str, vessels+dot+getConfigProperty(ConfigConstants.UUID));
		
		return reply;
	}

	/**
	 * Config defaults
	 * 
	 * @param props
	 */
	public static void setDefaults(SortedMap<String, Json> model) {
		// populate sensible defaults here
		model.put(ConfigConstants.UUID, Json.make(SignalKConstants.URN_UUID + UUID.randomUUID().toString()));
		model.put(ConfigConstants.WEBSOCKET_PORT, Json.make(8080));
		model.put(ConfigConstants.REST_PORT, Json.make(8080));
		model.put(ConfigConstants.WEBSOCKET_PORT_SSL, Json.make(8443));
		model.put(ConfigConstants.REST_PORT_SSL, Json.make(8443));
		model.put(ConfigConstants.STORAGE_ROOT, Json.make("./storage/"));
		model.put(ConfigConstants.STATIC_DIR, Json.make("./signalk-static/"));
		model.put(ConfigConstants.MAP_DIR, Json.make("./mapcache/"));
		model.put(ConfigConstants.DEMO, Json.make(true));
		model.put(ConfigConstants.DEMO_DELAY, Json.make(250));
		model.put(ConfigConstants.STREAM_URL, Json.make("motu.log"));
		model.put(ConfigConstants.USBDRIVE, Json.make("/media/usb0"));
		model.put(ConfigConstants.SERIAL_PORTS, Json.array(
				"/dev/ttyUSB0","/dev/ttyUSB1","/dev/ttyUSB2","/dev/ttyACM0","/dev/ttyACM1","/dev/ttyACM2"));
		if (SystemUtils.IS_OS_WINDOWS) {
			model.put(ConfigConstants.SERIAL_PORTS, Json.array("COM1","COM2","COM3","COM4"));
		}
		//jwt key
		//model.put(ConfigConstants.JWT_SECRET_KEY, Json.make(new String(Base64.encode(MacProvider.generateKey().getEncoded()))));
		model.put(ConfigConstants.SERIAL_PORT_BAUD, Json.make(38400));
		model.put(ConfigConstants.ENABLE_SERIAL, Json.make(true));
		model.put(ConfigConstants.TCP_PORT, Json.make(55555));
		model.put(ConfigConstants.UDP_PORT, Json.make(55554));
		model.put(ConfigConstants.TCP_NMEA_PORT, Json.make(55557));
		model.put(ConfigConstants.UDP_NMEA_PORT, Json.make(55556));
		model.put(ConfigConstants.STOMP_PORT, Json.make(61613));
		model.put(ConfigConstants.MQTT_PORT, Json.make(1883));
		model.put(ConfigConstants.CLOCK_SOURCE, Json.make("system"));//"gps"
		
		model.put(ConfigConstants.ARTEMIS_PERSIST, Json.make(false));

		model.put(ConfigConstants.HAWTIO_PORT, Json.make(8000));
		model.put(ConfigConstants.HAWTIO_AUTHENTICATE, Json.make(false));
		model.put(ConfigConstants.HAWTIO_CONTEXT, Json.make("/hawtio"));
		model.put(ConfigConstants.HAWTIO_WAR, Json.make("./hawtio/hawtio-default-offline-1.4.48.war"));
		model.put(ConfigConstants.HAWTIO_START, Json.make(false));

		model.put(ConfigConstants.JOLOKIA_PORT, Json.make(8001));
		model.put(ConfigConstants.JOLOKIA_AUTHENTICATE, Json.make(false));
		model.put(ConfigConstants.JOLOKIA_CONTEXT, Json.make("/jolokia"));
		model.put(ConfigConstants.JOLOKIA_WAR, Json.make("./hawtio/jolokia-war-1.3.3.war"));

		model.put(ConfigConstants.VERSION, Json.make("1.0.0"));
		model.put(ConfigConstants.ALLOW_INSTALL, Json.make(true));
		model.put(ConfigConstants.ALLOW_UPGRADE, Json.make(true));
		model.put(ConfigConstants.GENERATE_NMEA0183, Json.make(true));
		model.put(ConfigConstants.START_TCP, Json.make(true));
		model.put(ConfigConstants.START_UDP, Json.make(true));
		model.put(ConfigConstants.ZEROCONF_AUTO, Json.make(true));
		model.put(ConfigConstants.START_MQTT, Json.make(true));
		model.put(ConfigConstants.START_STOMP, Json.make(true));
		
		//security
		model.put(ConfigConstants.SECURITY_SSL_ENABLE, Json.make(true));
		model.put(ConfigConstants.SECURITY_PRIVATE_KEY, Json.make("./conf/test-private.pem"));
		model.put(ConfigConstants.SECURITY_CERTIFICATE, Json.make("./conf/test-cert.pem"));
		
		// control config, only local networks
		Json ips = Json.array();
		Enumeration<NetworkInterface> interfaces;
		try {
			interfaces = NetworkInterface.getNetworkInterfaces();

			while (interfaces.hasMoreElements()) {
				NetworkInterface i = interfaces.nextElement();
				for (InterfaceAddress iAddress : i.getInterfaceAddresses()) {
					// ignore IPV6 for now.
					if (iAddress.getAddress().getAddress().length > 4)
						continue;
					ips.add(iAddress.getAddress().getHostAddress() + "/" + iAddress.getNetworkPrefixLength());
				}
			}
			model.put(ConfigConstants.SECURITY_CONFIG, Json.make(ips.toString()));

			// default users
			model.put(ADMIN_USER, Json.make("admin"));
			model.put(ADMIN_PWD, Json.make("admin"));

		} catch (SocketException e) {
			logger.error(e.getMessage(), e);
		}

	}

	public static String getVersion() {
		return Config.getConfigProperty(ConfigConstants.VERSION);
	}

	public static NavigableMap<String, Json> loadConfig(NavigableMap<String, Json> model) throws IOException {
		
		logger.info("Loaded config defaults");
		
		logger.info("Loading saved config");
		influx.loadConfig(model,null );
		
		// ensure a uuid
		String selfUuid = model.get(ConfigConstants.UUID).asString();
		//create a self vessel
		model.put(vessels+dot+selfUuid+dot+uuid, Json.make(selfUuid));
		//saveConfig(model);
		if (logger.isDebugEnabled())logger.debug("Config: {}",model);
		return model;
	}


	/**
	 * Save the current state of the signalk config
	 * 
	 * @throws IOException
	 */
	public static void saveConfig(NavigableMap<String, Json> config) throws IOException {
		influx.save(config);
	}
	public static void saveConfig() throws IOException {
		influx.save(map);
	}

	public static String getConfigProperty(String prop) {
		try {
			if(map.get(prop)==null && defaultMap.get(prop)!=null) {
				map.put(prop, defaultMap.get(prop));
			}
			return (String) map.get(prop).getValue();
		} catch (Exception e) {
			logger.warn("getConfigProperty {} : {}",prop ,e.getMessage());
			if (logger.isDebugEnabled())logger.debug(e,e);
		}
		return null;
	}

	public static Json getConfigJsonArray(String prop) {
		if(map.get(prop)==null && defaultMap.get(prop)!=null) {
			map.put(prop, defaultMap.get(prop));
		}
		return map.get(prop);

	}

	public static Integer getConfigPropertyInt(String prop) {
		try {
			if(map.get(prop)==null && defaultMap.get(prop)!=null) {
				map.put(prop, defaultMap.get(prop));
			}
			return map.get(prop).asInteger();
		} catch (Exception e) {
			logger.warn("getConfigProperty {} : {}",prop ,e.getMessage());
			if (logger.isDebugEnabled())logger.debug(e,e);
		}
		return null;

	}

	public static Double getConfigPropertyDouble(String prop) {

		try {
			if(map.get(prop)==null && defaultMap.get(prop)!=null) {
				map.put(prop, defaultMap.get(prop));
			}
			return map.get(prop).asDouble();
		} catch (Exception e) {
			logger.warn("getConfigProperty {} : {}",prop ,e.getMessage());
			if (logger.isDebugEnabled())logger.debug(e,e);
		}
		return null;
	}

	public static Boolean getConfigPropertyBoolean(String prop) {
		try {
			if(map.get(prop)==null && defaultMap.get(prop)!=null) {
				map.put(prop, defaultMap.get(prop));
			}
			return map.get(prop).asBoolean();
		} catch (Exception e) {
			logger.warn("getConfigProperty {} : {}",prop ,e.getMessage());
			if (logger.isDebugEnabled())logger.debug(e,e);
		}
		return null;
	}

}
