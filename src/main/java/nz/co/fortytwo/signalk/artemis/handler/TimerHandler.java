package nz.co.fortytwo.signalk.artemis.handler;

import mjson.Json;
import nz.co.fortytwo.signalk.artemis.util.Util;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

import static nz.co.fortytwo.signalk.artemis.util.Config.*;
import static nz.co.fortytwo.signalk.artemis.util.SignalKConstants.*;


public class TimerHandler extends BaseHandler {


	private static Logger logger = LogManager.getLogger(LogbookAutoHandler.class);
	private static NavigableMap<String, Json> timerConditionMap = new ConcurrentSkipListMap<>();

	private static AtomicInteger timerCounter = new AtomicInteger(0);

	private static final int defaultTimerValue = 1800; // in seconds = 30 minutes
	private static final String conditionTimerID = "conditionTimerID"; // ID for condition "restarting timer when timer 0"
	private static final String conditionLogbookEntryID = "conditionLogbookEntryID"; /// ID for condition "send auto logbookEntry message when timer 0"

	public TimerHandler() {
		super();
		if (logger.isDebugEnabled())
			logger.debug("Initialising for : {} ", uuid);
		try {
			initSession(AMQ_INFLUX_KEY+" LIKE '%"+nav_datetime+"%' OR "
							+AMQ_INFLUX_KEY+" LIKE '%"+timer+"%'");
			// Creates a Timer set on defaultTimerValue; if timer expires, message for logbookentry auto will get send and timer will be restarted
			createDefaultTimerWithLogbookEntry();
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
		} else if(keyBeforeValues.endsWith("timer.set")) {
			timerCounter.set(value.at("setTimer").asInteger());
		} else if (keyBeforeValues.endsWith("timer.condition.set")) {
			if(value.has("action") && (value.has("id"))) {
				timerConditionMap.put(value.at("id").asString(), value);
			}
		} else if(keyBeforeValues.endsWith("timer.condition.delete")) {
			if(value.has("id") && timerConditionMap.containsKey(value.at("id").asString())) {
				timerConditionMap.remove(value.at("id").asString());
			}
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
		Json timerValueJson = Json.read("{\"id\":\"" + conditionTimerID + "\", \"onTimerValue\": 0, \"action\": \"setTimer\", \"setTimer\":"+ defaultTimerValue +"}");
		timerConditionMap.put(timerValueJson.at("id").asString(), timerValueJson);
		Json logbookValueJson = Json.read("{\"id\":\"" + conditionLogbookEntryID + "\", \"onTimerValue\": 0, \"action\" : \"logbookEntry\"}");
		timerConditionMap.put(logbookValueJson.at("id").asString(), logbookValueJson);

		timerCounter.set(defaultTimerValue);
	}

	public int getTimerCounter() {
		return timerCounter.get();
	}

	public NavigableSet<String> getIdsOfConditions() {
		return timerConditionMap.navigableKeySet();
	}

}