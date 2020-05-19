package nz.co.fortytwo.signalk.artemis.handler;

import mjson.Json;
import nz.co.fortytwo.signalk.artemis.tdb.LogbookDbService;
import nz.co.fortytwo.signalk.artemis.util.Util;
import org.apache.activemq.artemis.api.core.Message;
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

	public LogbookAutoHandler() {
		super();
		if (logger.isDebugEnabled())
			logger.debug("Initialising for : {} ", uuid);
		try {
			initSession(AMQ_INFLUX_KEY + " LIKE '%" + logbook + dot + auto + "%'");
			// Start new thread
			// Get last auto entry from DB
			// lastAutoEntryTime = influx.getLastEntry();
			logbookInfluxDB = new LogbookDbService();
			//logbookAutoThread.start();
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

}
