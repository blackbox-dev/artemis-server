package nz.co.fortytwo.signalk.artemis.handler;

import mjson.Json;
import nz.co.fortytwo.signalk.artemis.tdb.LogbookDbService;
import nz.co.fortytwo.signalk.artemis.util.Util;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;

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

			initSession(
					AMQ_INFLUX_KEY + " LIKE '" + logbook + dot + event + "%' OR "
					+ AMQ_INFLUX_KEY + " LIKE '%" + notifications + "%'"
			);

			logbookInfluxDB = new LogbookDbService(); // initialize logbook database service
			logbookEvent = LogbookEvents.NULL; // initialize logbook event to NULL
		} catch (Exception e) {
			logger.error(e, e);
		}
	}

	@Override
	public void consume(Message message) {

		if (!AMQ_CONTENT_TYPE_JSON_DELTA.equals(message.getStringProperty(AMQ_CONTENT_TYPE)))
			return;

		Json node = Util.readBodyBuffer( message.toCore());

		// retrieve event type from message
		String eventTimestampArray[] = getEventAndTimestamp(node);
		logbookInfluxDB.saveToLogbook(eventTimestampArray[0]);
		return;
	}

	/**
	 * Extract the eventType and timestamp from the message.
	 * @Return The event type and timestamp as String[]
	 * */
	private String[] getEventAndTimestamp(Json node) {
		String[] evenTimestampArray = new String[2];
		// Differentiate between logbook event msg or vessels.notification
		if (node.at("value").has("message")) {
			// vessels.notification msg
			evenTimestampArray[0] = node.at("value").at("message").getValue().toString();
		} else {
			// logbook event msg
			String event = node.at("value").getValue().toString();
			String e = event.split("=")[0].replace("{","").trim();
			evenTimestampArray[0] = e;
		}
		evenTimestampArray[1] = node.at("timestamp").getValue().toString();
		//long epoch = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(timestamp).getTime() / 1000
		return evenTimestampArray;
	}
}