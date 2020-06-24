package nz.co.fortytwo.signalk.artemis.handler;

import nz.co.fortytwo.signalk.artemis.tdb.LogbookDbService;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
					//+ AMQ_INFLUX_KEY + " LIKE '" + vessels + "%'");
			logbookInfluxDB = new LogbookDbService();
		} catch (Exception e) {
			logger.error(e, e);
		}
	}

	@Override
	public void consume(Message message) {
		logbookInfluxDB.saveToLogbook("auto", System.currentTimeMillis() + "" );
	}

}