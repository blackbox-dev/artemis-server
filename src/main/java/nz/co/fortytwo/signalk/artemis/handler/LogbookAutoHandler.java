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
	private static NavigableMap<String, Json> alarmMap = new ConcurrentSkipListMap<>();

	private static final int THRESHOLD = 1; // in minutes
	// TODO: Change this to get last auto entry from DB
	private static LocalDateTime lastAutoEntryTime = LocalDateTime.now();

	public LogbookAutoHandler() {
		super();
		if (logger.isDebugEnabled())
			logger.debug("Initialising for : {} ", uuid);
		try {
			initSession(AMQ_INFLUX_KEY + " LIKE '" + logbook + dot + auto + "%'");
			// Start new thread
			// Get last auto entry from DB
			// lastAutoEntryTime = influx.getLastEntry();
			logbookInfluxDB = new LogbookDbService();
			Thread logbookAutoThread = new LogbookThreadRunner();
			//logbookAutoThread.start();
		} catch (Exception e) {
			logger.error(e, e);
		}
	}

	public  class LogbookThreadRunner extends Thread {
		@Override
		public void run() {
			System.out.println("LogbookThreadRunner");
			// keep thread alive
			while(true) {
				if(LocalDateTime.now().isAfter(lastAutoEntryTime.plusMinutes(THRESHOLD))) {
					// create auto entry in logbook
					logbookInfluxDB.saveToLogbook("auto");
					// update lastAutoEntryTime
					lastAutoEntryTime = LocalDateTime.now();

				}
				// if > threshold make auto entry
				// after auto entry sleep for threshold - 1minute Thread.sleep()
			}
		}
	}


	@Override
	public void consume(Message message) {

		String key = message.getStringProperty(AMQ_INFLUX_KEY);

		Json node = Util.readBodyBuffer( message.toCore());


		// TODO: process message
		logger.info("got message -- " + node.asList());
		return;
	}

}
