package nz.co.fortytwo.signalk.artemis.handler;

import mjson.Json;
import nz.co.fortytwo.signalk.artemis.util.Util;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static nz.co.fortytwo.signalk.artemis.util.Config.AMQ_INFLUX_KEY;
import static nz.co.fortytwo.signalk.artemis.util.SignalKConstants.*;

public class LogbookAutoHandler extends BaseHandler {

	private static Logger logger = LogManager.getLogger(LogbookAutoHandler.class);

	private static NavigableMap<String, Json> alarmMap = new ConcurrentSkipListMap<>();

	public LogbookAutoHandler() {
		super();
		if (logger.isDebugEnabled())
			logger.debug("Initialising for : {} ", uuid);
		try {
			initSession(AMQ_INFLUX_KEY+" LIKE '"+logbook+dot+auto+"%'");
		} catch (Exception e) {
			logger.error(e, e);
		}
	}


	@Override
	public void consume(Message message) {

		Json node = Util.readBodyBuffer( message.toCore());

		// TODO: process message
		logger.info("got message -- " + node.asList());
		return;
	}

}
