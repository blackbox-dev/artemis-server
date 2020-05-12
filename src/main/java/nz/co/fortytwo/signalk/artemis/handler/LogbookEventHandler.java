package nz.co.fortytwo.signalk.artemis.handler;

import mjson.Json;
import nz.co.fortytwo.signalk.artemis.tdb.LogbookDbService;
import nz.co.fortytwo.signalk.artemis.util.Util;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static nz.co.fortytwo.signalk.artemis.util.Config.*;
import static nz.co.fortytwo.signalk.artemis.util.SignalKConstants.*;

enum LogbookEvents {
	NULL,
	MOB,
	REEFING,
	FIRE,
	FLOODING
}

public class LogbookEventHandler extends BaseHandler {

	private static Logger logger = LogManager.getLogger(LogbookEventHandler.class);
	private LogbookDbService logbookInfluxDB;
	private LogbookEvents logbookEvent;
	private static NavigableMap<String, Json> alarmMap = new ConcurrentSkipListMap<>();

	public LogbookEventHandler() {
		super();
		if (logger.isDebugEnabled())
			logger.debug("Initialising for : {} ", uuid);
		try {
			initSession(AMQ_INFLUX_KEY+" LIKE '"+logbook+dot+event+"%'");
			logbookInfluxDB = new LogbookDbService(); // initialize logbook database service
			logbookEvent = LogbookEvents.NULL; // initialize logbook event
		} catch (Exception e) {
			logger.error(e, e);
		}
	}

	@Override
	public void consume(Message message) {

		// check if message is an event message.
		//if (!AMQ_CONTENT_TYPE_EVENT_TRIGGERED.equals(message.getStringProperty(AMQ_CONTENT_TYPE)))
		//	return;
		if (!AMQ_CONTENT_TYPE_JSON_DELTA.equals(message.getStringProperty(AMQ_CONTENT_TYPE)))
			return;

		Json node = Util.readBodyBuffer( message.toCore());

		// TODO: process message
		//logger.info("got message -- " + node.asList());

		// match message to appropriate event
		//logbookEvent = node.getEvent(); // something similar to extract event type from message
		//String event = node.at("value").asString();
		logbookEvent = LogbookEvents.MOB;
		logbookInfluxDB.saveToLogbook(logbookEvent.toString());
		return;
	}

}
