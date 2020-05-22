package nz.co.fortytwo.signalk.artemis.util;

import static nz.co.fortytwo.signalk.artemis.util.Config.ADMIN_PWD;
import static nz.co.fortytwo.signalk.artemis.util.Config.ADMIN_USER;
import static nz.co.fortytwo.signalk.artemis.util.Config.AMQ_CONTENT_TYPE;
import static nz.co.fortytwo.signalk.artemis.util.Config.AMQ_CORR_ID;
import static nz.co.fortytwo.signalk.artemis.util.Config.AMQ_INFLUX_KEY;
import static nz.co.fortytwo.signalk.artemis.util.Config.AMQ_REPLY_Q;
import static nz.co.fortytwo.signalk.artemis.util.Config.AMQ_SUB_DESTINATION;
import static nz.co.fortytwo.signalk.artemis.util.Config.AMQ_USER_TOKEN;
import static nz.co.fortytwo.signalk.artemis.util.Config.INCOMING_RAW;
import static nz.co.fortytwo.signalk.artemis.util.Config.INTERNAL_KV;
import static nz.co.fortytwo.signalk.artemis.util.Config.OUTGOING_REPLY;
import static nz.co.fortytwo.signalk.artemis.util.Config.getConfigProperty;
import static nz.co.fortytwo.signalk.artemis.util.SignalKConstants.dot;
import static nz.co.fortytwo.signalk.artemis.util.SignalKConstants.self_str;
import static nz.co.fortytwo.signalk.artemis.util.SignalKConstants.timestamp;
import static nz.co.fortytwo.signalk.artemis.util.SignalKConstants.value;
import static nz.co.fortytwo.signalk.artemis.util.SignalKConstants.version;

import java.util.NavigableMap;
import java.util.UUID;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import mjson.Json;

public class MessageSupport {

	private static Logger logger = LogManager.getLogger(MessageSupport.class);

	protected ThreadLocal<ClientProducer> producer = new ThreadLocal<>();

	protected ThreadLocal<ClientSession> txSession = new ThreadLocal<>();

	// protected ClientConsumer consumer;

	// protected static TDBService influx = new InfluxDbService();

	protected String uuid;

	public MessageSupport() {
		uuid = Config.getConfigProperty(ConfigConstants.UUID);
	}

	public void initSession() throws Exception {
		getTxSession();
		getProducer();

	}

	public void send(Message message, String key, double d) throws ActiveMQException {
		if (logger.isDebugEnabled())
			logger.debug("Sending: key: {}, value: {}", key, d);
		Json json = Json.object().set(value, d).set(timestamp, Util.getIsoTimeString());
		if (logger.isDebugEnabled())
			logger.debug("Sending: key: {}, value: {}", key, json);
		sendKvMessage(message, key, json);

	}

	public void sendJson(Message message, String key, Json json) throws ActiveMQException {
		if (!json.isNull() && !json.has(timestamp)) {
			json.set(timestamp, Util.getIsoTimeString());
		}
		if (logger.isDebugEnabled())
			logger.debug("Sending json: key: {}, value: {}", key, json);

		sendKvMessage(message, key, json);

	}

	public void sendKvMap(Message message, NavigableMap<String, Json> map) {
		// remove "self" and "version"
		map.remove(self_str);
		map.remove(version);

		map.forEach((k, j) -> {
			try {
				sendKvMessage(message, k, j);
			} catch (Exception e) {
				logger.error(e, e);
			}
		});

	}

	public void sendReply(String destination, String format, String correlation, Json json, String token)
			throws Exception {

		if (json == null || json.isNull())
			json = Json.object();

		ClientMessage txMsg = getTxSession().createMessage(false);

		// txMsg.putStringProperty(JAVA_TYPE, type);
		if (correlation != null)
			txMsg.putStringProperty(AMQ_CORR_ID, correlation);
		if (token != null)
			txMsg.putStringProperty(AMQ_USER_TOKEN, token);
		txMsg.putStringProperty(AMQ_SUB_DESTINATION, destination);

		txMsg.putStringProperty(SignalKConstants.FORMAT, format);
		txMsg.putBooleanProperty(SignalKConstants.REPLY, true);
		txMsg.setExpiration(System.currentTimeMillis() + 5000);
		txMsg.getBodyBuffer().writeString(json.toString());
		if (logger.isDebugEnabled())
			logger.debug("Destination: {}, Msg body = {}", OUTGOING_REPLY + dot + destination, json.toString());
		getProducer().send(OUTGOING_REPLY + dot + destination, txMsg);

	}

	protected String sendMessage(String queue, String body, String correlation, String jwtToken)
			throws ActiveMQException {
		if (logger.isDebugEnabled())
			logger.debug("Incoming msg: {}, {}", correlation, body);
		ClientMessage message = null;

		message = getTxSession().createMessage(false);

		message.getBodyBuffer().writeString(body);
		message.putStringProperty(AMQ_REPLY_Q, queue);
		if (correlation != null) {
			message.putStringProperty(AMQ_CORR_ID, correlation);
		}
		if (jwtToken != null) {
			message.putStringProperty(AMQ_USER_TOKEN, jwtToken);
		}
		send(new SimpleString(INCOMING_RAW), message);
		return correlation;
	}

	private void send(SimpleString queue, ClientMessage message) throws ActiveMQException {
		getProducer().send(queue, message);

	}

	protected void sendRawMessage(Message origMessage, String body) throws ActiveMQException {
		ClientMessage txMsg = getTxSession().createMessage(false);
		txMsg.copyHeadersAndProperties(origMessage);

		txMsg.removeProperty(AMQ_CONTENT_TYPE);

		txMsg.setExpiration(System.currentTimeMillis() + 5000);
		txMsg.getBodyBuffer().writeString(body);
		if (logger.isDebugEnabled())
			logger.debug("Msg body incoming.raw: {}", body);

		getProducer().send(INCOMING_RAW, txMsg);

	}

	public void sendKvMessage(Message origMessage, String k, Json j) throws ActiveMQException {
		ClientMessage txMsg = getTxSession().createMessage(false);
		txMsg.copyHeadersAndProperties(origMessage);
		txMsg.putStringProperty(AMQ_INFLUX_KEY, k);
		txMsg.setRoutingType(RoutingType.MULTICAST);
		txMsg.setExpiration(System.currentTimeMillis() + 5000);
		txMsg.getBodyBuffer().writeString(j.toString());
		if (logger.isDebugEnabled())
			logger.debug("Msg body signalk.kv: {} = {}", k, j.toString());

		//getProducer().send(INCOMING_RAW, txMsg);
		getProducer().send(INTERNAL_KV, txMsg);

	}

	public ClientSession getTxSession() {

		if (txSession == null)
			txSession = new ThreadLocal<>();

		if (txSession.get() == null) {
			if (logger.isDebugEnabled())
				logger.debug("Start amq session");
			try {
				txSession.set(Util.getVmSession(getConfigProperty(ADMIN_USER), getConfigProperty(ADMIN_PWD)));
			} catch (Exception e) {
				logger.error(e, e);
			}
		}
		return txSession.get();
	}

	public ClientProducer getProducer() {

		if (producer == null)
			producer = new ThreadLocal<>();

		if (producer.get() == null && getTxSession() != null && !getTxSession().isClosed()) {
			if (logger.isDebugEnabled())
				logger.debug("Start producer");

			try {
				producer.set(getTxSession().createProducer());
			} catch (ActiveMQException e) {
				logger.error(e, e);
			}

		}
		return producer.get();

	}

	protected ClientMessage initClientMessage() throws Exception {
		ClientMessage txMsg = null;
		txMsg = getTxSession().createMessage(true);
		txMsg.putStringProperty(Config.MSG_SRC_BUS, "self.internal");
		txMsg.putStringProperty(Config.MSG_SRC_TYPE, Config.MSG_SRC_TYPE_SERIAL);
		txMsg.putStringProperty(Config.AMQ_CONTENT_TYPE, Config.AMQ_CONTENT_TYPE_JSON);
		String token = SecurityUtils.authenticateUser("admin", "admin");
		txMsg.putStringProperty(Config.AMQ_USER_ROLES, SecurityUtils.getRoles(token).toString());
		return txMsg;
	}

	public void stop() {

		if (producer != null && producer.get() != null) {
			try {
				producer.get().close();
				producer.remove();
			} catch (ActiveMQException e) {
				logger.error(e, e);
			}
		}
		if (txSession != null && txSession.get() != null) {
			try {
				txSession.get().close();
				txSession.remove();
			} catch (ActiveMQException e) {
				logger.error(e, e);
			}
		}

	}

	public String getToken(Json authRequest) {
		if (authRequest != null) {
			if (authRequest.has("login") && authRequest.at("login").has("token")) {
				return authRequest.at("login").at("token").asString();
			}
			if (authRequest.has("logout") && authRequest.at("logout").has("token")) {
				return authRequest.at("logout").at("token").asString();
			}
		}
		return null;
	}

	public String getRequestId(Json authRequest) {
		if (authRequest != null && authRequest.has("requestId")) {
			return authRequest.at("requestId").asString();
		}
		return UUID.randomUUID().toString();
	}

	public int getResultCode(Json authRequest) {
		if (authRequest != null && authRequest.has("result")) {
			return authRequest.at("result").asInteger();
		}
		return 500;
	}

	public Json reply(String requestId, String state, int result) {
		return Json.object().set("requestId", requestId).set("state", state).set("result", result);
	}

	public Json error(String requestId, String state, int result, String message) {
		return reply(requestId, state, result).set("message", message);
	}

	@Override
	protected void finalize() throws Throwable {
		stop();
		super.finalize();
	}

	protected void setUuid(String uuid) {
		this.uuid = uuid;
	}

}
