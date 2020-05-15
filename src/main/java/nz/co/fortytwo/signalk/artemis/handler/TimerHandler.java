package nz.co.fortytwo.signalk.artemis.handler;

import mjson.Json;
import nz.co.fortytwo.signalk.artemis.tdb.LogbookDbService;
import nz.co.fortytwo.signalk.artemis.util.Config;
import nz.co.fortytwo.signalk.artemis.util.ConfigConstants;
import nz.co.fortytwo.signalk.artemis.util.Util;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
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

	public TimerHandler() {
		super();
		if (logger.isDebugEnabled())
			logger.debug("Initialising for : {} ", uuid);
		try {
			initSession(AMQ_INFLUX_KEY+" LIKE '%"+nav_datetime+"%' OR "
							+AMQ_INFLUX_KEY+" LIKE '%"+timer+"%'");
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
			int timerVal = timerCounter.getAndDecrement();
			System.out.println("TimerCount:" + timerVal);
			NavigableSet<String> idOfConditions = timerConditionMap.navigableKeySet();// Returns a reverse order NavigableSet

			for (String id : idOfConditions) {
				Json jsonValue = timerConditionMap.get(id);
				if (timerVal == jsonValue.at("timeValue").asInteger()) {
					String action = jsonValue.at("action").asString();
					Json notification = jsonValue.at("notification");
					if (action.equals("logbookEntry")) {
						Json notificationJson = buildNotificationJson(notification.at("state"), notification.at("method"), notification.at("message"));
						String idLogbook = "vessels." + uuid + dot + logbook + dot + auto + ".values.unknown";
						sendTMessage(message, idLogbook, notificationJson);
					} else if (action.equals("notification")) {
						Json notificationJson = buildNotificationJson(notification.at("state"), notification.at("method"), notification.at("message"));
						int len = Util.getContext(key).length();
						String keyNotification = key.substring(0, len) + dot + notifications + key.substring(len);
						sendTMessage(message, keyNotification, notificationJson);
					}
				}
			}
		} else if(keyBeforeValues.endsWith("timer.start")) {
			timerCounter.set(value.at("timeValue").asInteger());
		} else if(keyBeforeValues.endsWith("timer.stop")) {
			timerCounter.set(0);
		} else if (keyBeforeValues.endsWith("timer.condition.set")) {
			if(value.has("action") && (value.has("id"))) {
				timerConditionMap.put(value.at("id").asString(), value);
			}
		} else if(keyBeforeValues.endsWith("timer.condition.delete")) {
			if(value.has("id") && timerConditionMap.containsKey(value.at("id"))) {
				timerConditionMap.remove(value.at("id"));
			}
		}
	}

	private void sendTMessage(Message message, String key, Json value) {

		/* needed ????
		String key = "notification.timerMessage";
		String source = "timer";
		message.putStringProperty(Config.AMQ_INFLUX_KEY,"vessels."+uuid+"."+key+".values."+source); // needed??
		 */

		// TODO: find out -> sendKvMessage or sendJson ???
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

	public int getTimerCounter() {
		return timerCounter.get();
	}

	public NavigableSet<String> getIdsOfConditions() {
		return timerConditionMap.navigableKeySet();
	}

}

// implementation s seperate timer
/*

		if(timeCounter == 30) {
			timeCounter = 0;
			String timeStamp = node.at(value).toString();
			System.out.println(timeStamp);
		}
 */