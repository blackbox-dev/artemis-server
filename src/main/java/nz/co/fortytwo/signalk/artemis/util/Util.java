package nz.co.fortytwo.signalk.artemis.util;

import static nz.co.fortytwo.signalk.util.SignalKConstants.CONTEXT;
import static nz.co.fortytwo.signalk.util.SignalKConstants.PATH;
import static nz.co.fortytwo.signalk.util.SignalKConstants.UPDATES;
import static nz.co.fortytwo.signalk.util.SignalKConstants.dot;
import static nz.co.fortytwo.signalk.util.SignalKConstants.label;
import static nz.co.fortytwo.signalk.util.SignalKConstants.self_str;
import static nz.co.fortytwo.signalk.util.SignalKConstants.source;
import static nz.co.fortytwo.signalk.util.SignalKConstants.sourceRef;
import static nz.co.fortytwo.signalk.util.SignalKConstants.sources;
import static nz.co.fortytwo.signalk.util.SignalKConstants.timestamp;
import static nz.co.fortytwo.signalk.util.SignalKConstants.value;
import static nz.co.fortytwo.signalk.util.SignalKConstants.values;
import static nz.co.fortytwo.signalk.util.SignalKConstants.version;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.regex.Pattern;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQPropertyConversionException;
import org.apache.activemq.artemis.api.core.ActiveMQSecurityException;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.client.impl.ClientMessageImpl;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMConnectorFactory;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import mjson.Json;
import nz.co.fortytwo.signalk.util.ConfigConstants;
import nz.co.fortytwo.signalk.util.JsonSerializer;

public class Util extends nz.co.fortytwo.signalk.util.Util {

	static Logger logger = LogManager.getLogger(Util.class);
	// private static Pattern selfMatch = Pattern.compile("\\.self\\.");
	// private static Pattern selfEndMatch = Pattern.compile("\\.self$");
	public static final String SIGNALK_MODEL_SAVE_FILE = "./conf/self.json";
	public static final String SIGNALK_CFG_SAVE_FILE = "./conf/signalk-config.json";
	public static final String SIGNALK_RESOURCES_SAVE_FILE = "./conf/resources.json";
	public static final String SIGNALK_SOURCES_SAVE_FILE = "./conf/sources.json";
	private static ServerLocator nettyLocator;
	private static ServerLocator inVmLocator;
	protected static Pattern selfMatch = null;

	protected static Pattern selfEndMatch = null;

	static {
		try {
			inVmLocator = ActiveMQClient
					.createServerLocatorWithoutHA(new TransportConfiguration(InVMConnectorFactory.class.getName()))
					.setMinLargeMessageSize(1024 * 1024);
			// .createSessionFactory();
			Map<String, Object> connectionParams = new HashMap<String, Object>();
			connectionParams.put(TransportConstants.HOST_PROP_NAME, "localhost");
			connectionParams.put(TransportConstants.PORT_PROP_NAME, 61617);
			nettyLocator = ActiveMQClient
					.createServerLocatorWithoutHA(
							new TransportConfiguration(NettyConnectorFactory.class.getName(), connectionParams))
					.setMinLargeMessageSize(1024 * 1024);
			// .createSessionFactory();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static Json getWelcomeMsg() {
		Json msg = Json.object();
		msg.set(version, Config.getVersion());
		msg.set(timestamp, getIsoTimeString());
		msg.set(self_str, Config.getConfigProperty(ConfigConstants.UUID));
		return msg;
	}

	/**
	 * If we receive messages for our UUID, convert to 'self'
	 * 
	 * @param key
	 * @return
	 */
	public static String fixSelfKey(String key) {
		if (selfMatch == null) {
			selfMatch = Pattern.compile("\\." + Config.getConfigProperty(ConfigConstants.UUID) + "\\.");
			selfEndMatch = Pattern.compile("\\." + Config.getConfigProperty(ConfigConstants.UUID) + "$");
		}
		key = selfMatch.matcher(key).replaceAll(".self.");

		key = selfEndMatch.matcher(key).replaceAll(".self");
		return key;
	}

	public static ClientSession getVmSession(String user, String password) throws Exception {

		return inVmLocator.createSessionFactory().createSession(user, password, false, true, true, false, 10);
	}

	public static ClientSession getLocalhostClientSession(String user, String password) throws Exception {

		return nettyLocator.createSessionFactory().createSession(user, password, false, true, true, false, 10);
	}

	public static void sendDoubleAsMsg(String key, double value, String timeStamp, String srcRef, ServerSession session)
			throws Exception {
		if (StringUtils.isNotBlank(srcRef)) {
			sendObjMsg(key + dot + values + dot + srcRef, Json.make(value), timeStamp, srcRef, session);
		} else {
			sendObjMsg(key, Json.make(value), timeStamp, srcRef, session);
		}
	}

	public static SortedMap<String, Object> readAllMessages(String user, String password, String queue, String filter) throws Exception  {
		SortedMap<String, Object> msgs = null;
		ClientSession rxSession = null;
		ClientConsumer consumer = null;
		try {
			// start polling consumer.
			rxSession = Util.getVmSession(user, password);
			rxSession.start();
			consumer = rxSession.createConsumer(queue, filter, true);

			msgs = readAllMessages(consumer);// new
																		// ConcurrentSkipListMap<>();
			consumer.close();
		
		} catch (ActiveMQException e) {
			logger.error(e);
		} finally {
			if (consumer != null) {
				try {
					consumer.close();
				} catch (ActiveMQException e) {
					logger.error(e);
				}
			}
			if (rxSession != null) {
				try {
					rxSession.close();
				} catch (ActiveMQException e) {
					logger.error(e);
				}
			}
		}
		if(msgs !=null){
			return msgs;
		}
		return new ConcurrentSkipListMap<String, Object>();
	}

	

	public static void sendRawMessage(String user, String password, String content) throws Exception {
		ClientSession txSession = null;
		ClientProducer producer = null;
		try {
			// start polling consumer.
			txSession = Util.getVmSession(user, password);
			Message message = txSession.createMessage(false);
			message.getBodyBuffer().writeString(content);
			producer = txSession.createProducer();
			producer.send(Config.INCOMING_RAW, message);
		} finally {
			if (producer != null) {
				try {
					producer.close();
				} catch (ActiveMQException e) {
					logger.error(e);
				}
			}
			if (txSession != null) {
				try {
					txSession.close();
				} catch (ActiveMQException e) {
					logger.error(e);
				}
			}
		}
	}

	public static void sendMsg(String key, Json body, String timeStamp, String srcRef, ServerSession sess)
			throws Exception {
		if (StringUtils.isNotBlank(srcRef) && !key.contains(values)) {
			sendObjMsg(key + dot + values + dot + srcRef, body, timeStamp, srcRef, sess);
		} else {
			sendObjMsg(key, body, timeStamp, srcRef, sess);
		}
	}

	public static void sendMsg(String key, Json body, String timeStamp, Json src, ServerSession sess) throws Exception {
		if (src != null && !src.isNull()) {
			String srclabel = src.at(label).asString();
			if (srclabel.startsWith(sources))
				srclabel = srclabel.substring(sources.length() + 1);
			sendObjMsg(key + dot + values + dot + srclabel, body, timeStamp, src, sess);
		} else {
			sendObjMsg(key, body, timeStamp, src, sess);
		}
	}

	private static void sendObjMsg(String key, Json body, String timeStamp, Object src, ServerSession sess)
			throws Exception {

		ClientMessage m2 = new ClientMessageImpl((byte)0, false, 0, System.currentTimeMillis(), (byte) 4, 1024); 
		if(StringUtils.isNotBlank(timeStamp))
			m2.putStringProperty(timestamp, timeStamp);
		if (src != null) {
			if (src instanceof String) {
				m2.putStringProperty(sourceRef, src.toString());
			} else {
				m2.putStringProperty(source, src.toString());
			}
		}
		String type = body.getClass().getSimpleName();
		m2.putStringProperty(Config.JAVA_TYPE, type);

		switch (type) {
		case "NullJson":
			m2.getBodyBuffer().writeString(body.toString());
			m2.putStringProperty(Config.SK_TYPE, Config.SK_TYPE_VALUE);
			break;
		case "BooleanJson":
			m2.getBodyBuffer().writeString(body.toString());
			m2.putStringProperty(Config.SK_TYPE, Config.SK_TYPE_VALUE);
			break;
		case "StringJson":
			m2.getBodyBuffer().writeString(body.toString());
			m2.putStringProperty(Config.SK_TYPE, Config.SK_TYPE_VALUE);
			break;
		case "NumberJson":
			m2.getBodyBuffer().writeString(body.toString());
			m2.putStringProperty(Config.SK_TYPE, Config.SK_TYPE_VALUE);
			break;
		case "Json":
			m2.getBodyBuffer().writeString(body.toString());
			m2.putStringProperty(Config.SK_TYPE, Config.SK_TYPE_COMPOSITE);
			break;
		case "ObjectJson":
			m2.getBodyBuffer().writeString(body.toString());
			m2.putStringProperty(Config.SK_TYPE, Config.SK_TYPE_COMPOSITE);
			break;
		case "ArrayJson":
			m2.getBodyBuffer().writeString(body.toString());
			m2.putStringProperty(Config.SK_TYPE, Config.SK_TYPE_COMPOSITE);
			break;
		default:
			logger.error("Unknown Json Class type: " + type);
			m2.putStringProperty(Config.SK_TYPE, Config.SK_TYPE_VALUE);
			m2.getBodyBuffer().writeString(body.toString());
			break;
		}

		m2.setAddress(new SimpleString(key));
		m2.putStringProperty(Config._AMQ_LVQ_NAME, key);

		try {
			sess.send(m2, true);
		} catch (ActiveMQSecurityException se) {
			logger.warn(se.getMessage());
		} catch (Exception e1) {
			logger.error(e1.getMessage(), e1);
		}
	}

	public static void sendSourceMsg(String key, String src, String now, ServerSession sess) throws Exception {
		sendObjMsg("sources." + key, Json.read(src), now, null, sess);

	}

	public static void sendSourceMsg(String key, Json src, String now, ServerSession sess) throws Exception {
		sendObjMsg("sources." + key, src, now, null, sess);

	}

	public static String sanitizePath(String newPath) {
		newPath = newPath.replace('/', '.');
		if (newPath.startsWith(dot)) {
			newPath = newPath.substring(1);
		}
		if (newPath.endsWith("*") || newPath.endsWith("?")) {
			newPath = newPath.substring(0, newPath.length() - 1);
		}

		return newPath;
	}

	/**
	 * Input is a list of message wrapped in a stack of hashMaps, eg Key=context
	 * Key=timestamp key=source List(messages) The method iterates through and
	 * creates the deltas as a Json array, one Json delta per context.
	 * 
	 * @param msgs
	 * @return
	 */
	public static Json generateDelta(Map<String, Map<String, Map<String, List<ClientMessage>>>> msgs) {
		Json deltaArray = Json.array();
		// add values
		if (msgs.size() == 0)
			return deltaArray;
		// each timestamp
		Json delta = Json.object();
		deltaArray.add(delta);

		Json updatesArray = Json.array();
		delta.set(UPDATES, updatesArray);

		for (String ctx : msgs.keySet()) {

			for (String ts : msgs.get(ctx).keySet()) {
				for (String src : msgs.get(ctx).get(ts).keySet()) {
					// new values object

					// make wrapper object
					Json valObj = Json.object();
					updatesArray.add(valObj);

					Json valuesArray = Json.array();
					valObj.at(values, valuesArray);
					// add timestamp
					valObj.set(timestamp, ts);
					// if(src.contains("{"))
					// logger.debug("GenerateDelta:src: "+src);
					valObj.set(sourceRef, src);
					// else
					// valObj.set(sourceRef, src);
					// now the values
					for (ClientMessage msg : msgs.get(ctx).get(ts).get(src)) {
						String key = msg.getAddress().toString().substring(ctx.length() + 1);
						if (key.contains(dot + values + dot))
							key = key.substring(0, key.indexOf(dot + values + dot));
						Json v = Util.readBodyBuffer(msg);
						if (logger.isDebugEnabled())
							logger.debug("Key: " + key + ", value: " + v);
						Json val = Json.object(PATH, key);
						val.set(value, v);
						valuesArray.add(val);
					}
				}
				// add context
			}
			delta.set(CONTEXT, ctx);
		}

		return deltaArray;
	}

	public static Json readBodyBuffer(ICoreMessage msg) {
		if (msg.getBodyBuffer().readableBytes() == 0) {
			if (logger.isDebugEnabled())
				logger.debug("Empty msg: " + msg.getAddress() + ": " + msg.getBodyBuffer().readableBytes());
			return Json.nil();
		}
		return Json.read(readBodyBufferToString(msg));

	}

	public static String readBodyBufferToString(ICoreMessage msg) {
		if (msg.getBodyBuffer().readableBytes() == 0) {
			return null;
		} else {
			return msg.getBodyBuffer().duplicate().readString();
		}

	}

	public static Map< String, Map<String, Map<String,List<ClientMessage>>>> readAllMessagesForDelta(ClientConsumer consumer) 
			throws ActiveMQPropertyConversionException, ActiveMQException{
		ClientMessage msgReceived = null;
		Map< String, Map<String, Map<String,List<ClientMessage>>>> msgs = new HashMap<>();
		while ((msgReceived = consumer.receive(10)) != null) {
			if(logger.isDebugEnabled())logger.debug("message = "  + msgReceived.getMessageID()+":" + msgReceived.getAddress() );
			String ctx = Util.getContext(msgReceived.getAddress().toString());
			Map< String,Map<String,List<ClientMessage>>> ctxMap = msgs.get(ctx);
			if(ctxMap==null){
				ctxMap=new HashMap<>();
				msgs.put(ctx, ctxMap);
			}
			Map<String,List<ClientMessage>> tsMap = ctxMap.get(msgReceived.getStringProperty(timestamp));
			if(tsMap==null){
				tsMap=new HashMap<>();
				ctxMap.put(msgReceived.getStringProperty(timestamp), tsMap);
			}
			if(logger.isDebugEnabled())logger.debug("$source: "+msgReceived.getStringProperty(sourceRef));
			List<ClientMessage> srcMap = tsMap.get(msgReceived.getStringProperty(sourceRef));
			if(srcMap==null){
				srcMap=new ArrayList<>();
				tsMap.put(msgReceived.getStringProperty(sourceRef), srcMap);
			}
			srcMap.add( msgReceived);
		}
		return msgs;
	}
	public static SortedMap<String, Object> readAllMessages(ClientConsumer consumer)
			throws ActiveMQPropertyConversionException, ActiveMQException {
		ClientMessage msgReceived = null;
		SortedMap<String, Object> msgs = new ConcurrentSkipListMap<>();
		while ((msgReceived = consumer.receive(10)) != null) {
			String key = msgReceived.getAddress().toString();
			if (logger.isDebugEnabled())
				logger.debug("message = " + msgReceived.getMessageID() + ":" + key);
			String ts = msgReceived.getStringProperty(timestamp);
			String src = msgReceived.getStringProperty(source);
			if (ts != null)
				msgs.put(key + dot + timestamp, ts);
			if (src != null)
				msgs.put(key + dot + source, src);
			if (ts == null && src == null) {
				msgs.put(key, Util.readBodyBuffer(msgReceived));
			} else {
				msgs.put(key + dot + value, Util.readBodyBuffer(msgReceived));
			}

		}
		return msgs;
	}

	/**
	 * Convert map to a json object
	 * @param msgs
	 * @return
	 * @throws IOException
	 */
	public static Json mapToJson(SortedMap<String, Object> msgs) throws IOException {
		Json json = null;
		if (msgs.size() > 0) {
			JsonSerializer ser = new JsonSerializer();
			json = Json.read(ser.write(msgs));
			if (logger.isDebugEnabled())
				logger.debug("json = " + json.toString());
		}
		return json;
	}

	public static void sendMessage(ClientSession session, ClientProducer producer, String address, String body) throws ActiveMQException {
		ClientMessage msg = session.createMessage(true);
		msg.getBodyBuffer().writeString(body);
		producer.send(address, msg);
		
	}
}
