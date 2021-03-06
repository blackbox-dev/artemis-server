package nz.co.fortytwo.signalk.artemis.handler;

import mjson.Json;
import nz.co.fortytwo.signalk.artemis.tdb.LogbookDbService;
import nz.co.fortytwo.signalk.artemis.util.Util;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static nz.co.fortytwo.signalk.artemis.util.Config.*;
import static nz.co.fortytwo.signalk.artemis.util.SignalKConstants.*;

public class LogbookEventHandler extends BaseHandler {

	private static Logger logger = LogManager.getLogger(LogbookEventHandler.class);
	private LogbookDbService logbookInfluxDB;
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
		logbookInfluxDB.saveToLogbook(eventTimestampArray[0], eventTimestampArray[1]);
		return;
	}

	/**
	 * Extract the eventType and timestamp from the message.
	 * @Return The event type and timestamp as String[]
	 * */
	private String[] getEventAndTimestamp(Json node) {
		String[] eventTimestampArray = new String[3];
		eventTimestampArray[0] = node.at("value").at("message").getValue().toString();
		eventTimestampArray[1] = node.at("value").at("timestamp").getValue().toString();
		// Differentiate between logbook event msg or vessels.notification
		/*if (node.at("value").has("message")) {
			// vessels.notification msg
			evenTimestampArray[0] = node.at("value").at("message").getValue().toString();
		} else {
			// logbook event msg
			String event = node.at("value").getValue().toString();
			String e = event.split("=")[0].replace("{","").trim();
			evenTimestampArray[0] = e;
		}*/
		eventTimestampArray[2] = node.at("timestamp").getValue().toString();
		//long epoch = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(timestamp).getTime() / 1000
		return eventTimestampArray;
	}
}