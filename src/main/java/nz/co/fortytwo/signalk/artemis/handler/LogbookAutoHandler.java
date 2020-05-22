package nz.co.fortytwo.signalk.artemis.handler;

import mjson.Json;
import nz.co.fortytwo.signalk.artemis.tdb.LogbookDbService;
import nz.co.fortytwo.signalk.artemis.util.Config;
import nz.co.fortytwo.signalk.artemis.util.SecurityUtils;
import nz.co.fortytwo.signalk.artemis.util.Util;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static nz.co.fortytwo.signalk.artemis.util.Config.AMQ_INFLUX_KEY;
import static nz.co.fortytwo.signalk.artemis.util.SignalKConstants.*;

public class LogbookAutoHandler extends BaseHandler {

	private static Logger logger = LogManager.getLogger(LogbookAutoHandler.class);
	private LogbookDbService logbookInfluxDB;

	private static String autoEntryId = "logbookAutoEntry";

	public LogbookAutoHandler() {
		super();
		if (logger.isDebugEnabled())
			logger.debug("Initialising for : {} ", uuid);
		try {
			initSession(AMQ_INFLUX_KEY + " LIKE '%" + logbook + dot + auto + "%'");

			logbookInfluxDB = new LogbookDbService();

			sendMessageToTimerForAutoEntry();

		} catch (Exception e) {
			logger.error(e, e);
		}
	}


	@Override
	public void consume(Message message) {

		String key = message.getStringProperty(AMQ_INFLUX_KEY);
		Json node = Util.readBodyBuffer( message.toCore());

		logbookInfluxDB.saveToLogbook("auto");

		return;
	}

	private void sendMessageToTimerForAutoEntry() throws Exception {
		// TODO: TimerHandler is not receiving this message only InfluxDbHandler
		Json timerConditionJson = Json.object();
		timerConditionJson.set("id", autoEntryId);
		timerConditionJson.set("timeValue", 0);
		timerConditionJson.set("action", "logbookEntry");
		timerConditionJson.set("notification", Json.object());
		timerConditionJson.at("notification").set("method", "[visual]").set("state", "normal").set("message", "logbook auto entry");

		ClientMessage message = null;
		try {
			message = initClientMessage();
			sendJson(message, "vessels." + uuid + dot + "timer.condition.set"+ ".values.unknown", timerConditionJson);

		} catch (ActiveMQException e) {
			logger.error(e, e);
		}
	}
}
