package nz.co.fortytwo.signalk.artemis.handler;

import mjson.Json;
import nz.co.fortytwo.signalk.artemis.util.Config;
import nz.co.fortytwo.signalk.artemis.util.SecurityUtils;
import nz.co.fortytwo.signalk.artemis.util.SignalKConstants;
import nz.co.fortytwo.signalk.artemis.util.Util;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.core.client.impl.ClientMessageImpl;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.ws.rs.core.MediaType;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static nz.co.fortytwo.signalk.artemis.util.Config.*;
import static nz.co.fortytwo.signalk.artemis.util.SignalKConstants.*;


public class TimerHandler extends BaseHandler {


	private static Logger logger = LogManager.getLogger(LogbookAutoHandler.class);
	private static NavigableMap<String, Json> timerConditionMap = new ConcurrentSkipListMap<>();

	private static AtomicInteger timerCounter = new AtomicInteger(0);

	private ClientMessage timerMessage = null;
	private static final int defaultTimerValue = 1800; // in seconds = 30 minutes
	private final AtomicBoolean doneInitMessages = new AtomicBoolean();

	public TimerHandler() {
		super();
		if (logger.isDebugEnabled())
			logger.debug("Initialising for : {} ", uuid);
		try {
			initSession(AMQ_INFLUX_KEY+" LIKE '%"+nav_datetime+"%' OR "
					+AMQ_INFLUX_KEY+" LIKE '%"+timer+".set%' OR "
					+AMQ_INFLUX_KEY+" LIKE '%"+timer+".condition%'");
			// Creates a Timer set on defaultTimerValue; if timer expires, message for logbookentry auto will get send and timer will be restarted
			timerMessage = getDefaultMessage();
		} catch (Exception e) {
			logger.error(e, e);
		}
	}

	@Override
	public void consume(Message message) {
		String key = message.getStringProperty(AMQ_INFLUX_KEY);
		String keyBeforeValues = StringUtils.substringBefore(key,dot+values+dot);
		Json node = Util.readBodyBuffer( message.toCore());
		Json value = node.at("value");
		createDefaultTimerWithLogbookEntry();
		if(key.endsWith("navigation.datetime")&& timerCounter.get() > 0){
			int timerVal = timerCounter.decrementAndGet();
			NavigableSet<String> idOfConditions = timerConditionMap.navigableKeySet();

			for (String id : idOfConditions) {
				Json jsonValue = timerConditionMap.get(id);
				if (timerVal == jsonValue.at("onTimerValue").asInteger()) {
					String action = jsonValue.at("action").asString();
					if (action.equals("logbookEntry")) {
						Json emptyJson = Json.object();
						emptyJson.set("value", null);
						String idLogbook = "vessels." + uuid + dot + logbook + dot + auto + ".values.unknown";
						sendTMessage(message, idLogbook, emptyJson);
					} else if (action.equals("notification")) {

						Json notification = jsonValue.at("notification");
						Json notificationJson = buildNotificationJson(notification.at("state"), notification.at("method"), notification.at("message"));
						int len = Util.getContext(key).length();
						String keyNotification = key.substring(0, len) + dot + notifications + key.substring(len);
						sendTMessage(message, keyNotification, notificationJson);
					}  else if (action.equals("setTimer")) {
						timerCounter.set(jsonValue.at("setTimer").asInteger());
					}
				}
			}
			sendTMessage(timerMessage, "vessels." + uuid + dot + timer + dot + "time",  Json.read("{\"value\":"+timerVal+"}"));

		} else if(keyBeforeValues.endsWith("timer.set")) {
			timerCounter.set(value.at("setTimer").asInteger());
		} else if (keyBeforeValues.endsWith("set")) { // "timer.condition.[id].set"))
			String keyParsedPre= StringUtils.substringBeforeLast(keyBeforeValues,".");
			String conditionId = StringUtils.substringAfterLast(keyParsedPre,".");
			if(value.has("action")) {
				timerConditionMap.put(conditionId, value);
			}
		} else if(keyBeforeValues.endsWith(".delete")) { // timer.condition.[id].delete"
			String keyParsedPre= StringUtils.substringBeforeLast(keyBeforeValues,".");
			String conditionId = StringUtils.substringAfterLast(keyParsedPre,".");
			sendTMessage(timerMessage, "vessels." + uuid + dot + "timer.condition."+conditionId+".set",  Json.read("{\"value\": null}"));
		}
	}

	private void sendTMessage(Message message, String key, Json value) {
		try {
			sendJson(message,key,value);
		} catch (ActiveMQException e) {
			logger.error(e, e);
		}
	}

	private Json buildNotificationJson(Json state, Json message, Json method) {
		Json notificationJson = Json.object();
		notificationJson.set("value", Json.object());
		notificationJson.at("value").set("method", method).set("state", state).set("message", message);
		return notificationJson;
	}

	private void createDefaultTimerWithLogbookEntry() {
		if (doneInitMessages.get()) return;
		if (doneInitMessages.compareAndSet(false, true)) {
			String keyD2 = "vessels." + uuid + dot + "timer.condition.defaultSetTimer.set";
			ClientMessage message2 = getDefaultMessage();
			Json jsonVal2 = Json.read("{\"value\":{\"onTimerValue\": 0, \"action\": \"setTimer\", \"setTimer\":"+ defaultTimerValue +"}}");
			sendTMessage(message2, keyD2, jsonVal2);

			String keyD1 = "vessels." + uuid + dot + "timer.condition.defaultLogbookEntry.set";
			ClientMessage message1 = getDefaultMessage();
			Json jsonVal1 = Json.read("{\"value\":{\"onTimerValue\": 0, \"action\" : \"logbookEntry\"}}");
			sendTMessage(message1, keyD1, jsonVal1);

			String keyD = "vessels." + uuid + dot + "timer.set";
			Json jsonVal = Json.read("{\"value\":{\"setTimer\":" +defaultTimerValue+"}}");
			sendTMessage(getDefaultMessage(), keyD, jsonVal);
		}
	}

	private ClientMessage getDefaultMessage() {
		ClientMessage txMsg = null;
		txMsg = getTxSession().createMessage(true);
		txMsg.putStringProperty(Config.MSG_SRC_BUS, "self.internal");
		txMsg.putStringProperty(Config.MSG_SRC_TYPE, Config.MSG_SRC_TYPE_INTERNAL_PROCESS);
		txMsg.putStringProperty(Config.AMQ_CONTENT_TYPE, Config.AMQ_CONTENT_TYPE_JSON);
		String token = null;
		try {
			token = SecurityUtils.authenticateUser("admin", "admin");
		} catch (Exception e) {
			e.printStackTrace();
		}
		txMsg.putStringProperty(Config.AMQ_USER_ROLES, SecurityUtils.getRoles(token).toString());
		txMsg.putStringProperty(Config.AMQ_INFLUX_KEY, "vessels."+uuid+"."+key);
		return txMsg;
	}
}