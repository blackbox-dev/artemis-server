package nz.co.fortytwo.signalk.artemis.service;

import static nz.co.fortytwo.signalk.artemis.util.ConfigConstants.SECURITY_SSL_ENABLE;
import static nz.co.fortytwo.signalk.artemis.util.ConfigConstants.STATIC_DIR;
import static nz.co.fortytwo.signalk.artemis.util.SecurityUtils.AUTH_COOKIE_NAME;
import static nz.co.fortytwo.signalk.artemis.util.SignalKConstants.SIGNALK_API;

import java.io.File;
import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Cookie;

import nz.co.fortytwo.signalk.artemis.util.*;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQIllegalStateException;
import org.apache.activemq.artemis.api.core.ActiveMQNonExistentQueueException;
import org.apache.activemq.artemis.api.core.ActiveMQQueueExistsException;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.MessageHandler;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.atmosphere.cpr.ApplicationConfig;
import org.atmosphere.cpr.AtmosphereResource;
import org.atmosphere.cpr.AtmosphereResourceEvent;
import org.atmosphere.cpr.AtmosphereResourceEventListenerAdapter;
import org.atmosphere.cpr.AtmosphereResourceSession;
import org.atmosphere.cpr.AtmosphereResourceSessionFactory;
import org.atmosphere.cpr.AtmosphereResponse;
import org.atmosphere.websocket.WebSocket;
import org.atmosphere.websocket.WebSocketEventListenerAdapter;

import mjson.Json;
import nz.co.fortytwo.signalk.artemis.subscription.SubscriptionManagerFactory;

public class BaseApiService extends MessageSupport{

	protected String tempQ;

	protected ClientConsumer consumer;

	private static Logger logger = LogManager.getLogger(BaseApiService.class);

	protected long lastBroadcast = System.currentTimeMillis();

	protected File staticDir = null;
	
	private static final long PING_PERIOD = 5000;
	
	private static Timer timer = new Timer();

	//protected BroadcasterFactory broadCasterFactory = new DefaultBroadcasterFactory();

	protected String scheme;

	public BaseApiService() {
		super();
		staticDir = new File(Config.getConfigProperty(STATIC_DIR));
		scheme = Config.getConfigPropertyBoolean(SECURITY_SSL_ENABLE)?"https":"http";
	}

	protected void initSession(String tempQ) throws Exception {
		if (logger.isDebugEnabled())
			logger.debug("initSession: {}", tempQ);
		if (getTempQ() == null) {
			this.tempQ = tempQ;
		}
		super.initSession();
		getConsumer();
	}

	public AtmosphereResource getResource(HttpServletRequest request) {
		return (AtmosphereResource) request.getAttribute(ApplicationConfig.ATMOSPHERE_RESOURCE);
	}
	
	public AtmosphereResponse getResponse(HttpServletRequest request) {
		return getResource(request).getResponse();
	}
	
	protected File getLogOutputFile(String logFile) {
		File installLogDir = new File(staticDir, "logs");
		installLogDir.mkdirs();
		// make log name
		File output = new File(installLogDir, logFile);
		return output;
	}

	protected String getToken(Cookie cookie) {
		if (cookie == null) {
			String token = null;
			try {
				token = SecurityUtils.authenticateUser("admin", "admin");
			} catch (Exception e) {
				e.printStackTrace();
			}
			return token;
		}
		// assume we might have a cookie token

		if (logger.isDebugEnabled())
			logger.debug("Cookie: {}, {}", cookie.getName(), cookie.getValue());
		if (AUTH_COOKIE_NAME.equals(cookie.getName())) {
			return cookie.getValue();
		}

		return null;
	}

	protected String addToken(String body, Cookie cookie) {
		return addToken(Json.read(body), cookie);
	}
	
	protected String addToken(Json body, Cookie cookie) {

		String jwtToken = getToken(cookie);
		if (StringUtils.isNotBlank(jwtToken)) {
			// add it to the body
			return body.set(SignalKConstants.TOKEN, jwtToken).toString();
		}
		return body.toString();
	}


	

	@Override
	protected void finalize() throws Throwable {
		closeSession();
		super.finalize();
	}

	protected void addCloseListener(AtmosphereResource resource) {

		resource.addEventListener(new AtmosphereResourceEventListenerAdapter() {

			@Override
			public void onThrowable(AtmosphereResourceEvent event) {
				try {
					event.getResource().close();
				} catch (NullPointerException e) {
					logger.error("NullPointerException: {}", e.getMessage());
				} catch (IllegalStateException | IOException e) {
					logger.error(e, e);
				}
				if (logger.isDebugEnabled()) {
					logger.debug("websocket.onThrowable: {}", event);
					event.throwable().printStackTrace();
				}

			}

			/**
			 * {@inheritDoc}
			 */
			@Override
			public void onDisconnect(AtmosphereResourceEvent event) {
				if (logger.isDebugEnabled())
					logger.debug("websocket.onDisconnect: {}", event);
				try {
					event.getResource().close();
				} catch (IllegalStateException | IOException e) {
					logger.error(e, e);
				}
				super.onDisconnect(event);
			}

			@Override
			public void onClose(AtmosphereResourceEvent event) {
				if (logger.isDebugEnabled())
					logger.debug("websocket.onClose: {}", event);
				closeSession();
				try {
					SubscriptionManagerFactory.getInstance().removeByTempQ(getTempQ());
				} catch (Exception e) {
					logger.error(e, e);
				}
				super.onClose(event);
			}

			@Override
			public void onBroadcast(AtmosphereResourceEvent event) {
				lastBroadcast = System.currentTimeMillis();
				if (logger.isDebugEnabled())
					logger.debug("websocket.onBroadcast: {}, {}", resource.uuid(), event);
				super.onBroadcast(event);
			}

			@Override
			public void onHeartbeat(AtmosphereResourceEvent event) {
				// lastBroadcast=System.currentTimeMillis();
				if (logger.isDebugEnabled())
					logger.debug("websocket.onHeartbeat, {}, {}", resource.uuid(), event);
				// super.onHeartbeat(event);
			}

		});
		if(logger.isDebugEnabled())
			logger.debug("Added close listener");
	}

	public boolean setConsumer(AtmosphereResource resource, boolean resumeAfter) throws ActiveMQException {
		return setConsumer(resource, resumeAfter, null);
	}
	
	public boolean setConsumer(AtmosphereResource resource, boolean resumeAfter, MessageHandler handler) throws ActiveMQException {

		if (getConsumer().getMessageHandler() == null) {
			if (logger.isDebugEnabled())
				logger.debug("Adding consumer messageHandler : {}", getTempQ());

			if (!resumeAfter) {
				resource.setBroadcaster(resource.getAtmosphereConfig().getBroadcasterFactory().get());
				logger.debug("Adding broadcaster");
			}
			if(handler!=null) {
				getConsumer().setMessageHandler(handler);
			}else {
				getConsumer().setMessageHandler(getDefaultMessageHandler(resource, resumeAfter));
			}
			logger.debug("Set handler");
			return true;
		}
		return false;
	}
	
	private MessageHandler getDefaultMessageHandler(AtmosphereResource resource, boolean resumeAfter) {
		MessageHandler handler = new MessageHandler() {

			@Override
			public void onMessage(ClientMessage message) {
				try {
					if (logger.isDebugEnabled())
						logger.debug("onMessage for client {}", message);
					String recv = Util.readBodyBufferToString(message);
					message.acknowledge();
					
					if (StringUtils.isBlank(recv))
						recv = "{}";

					if (logger.isDebugEnabled())
						logger.debug("onMessage for client at {}, {}", getTempQ(), recv);

					if (resumeAfter) {
						resource.write(recv);
						resource.resume();
					} else {
						resource.getBroadcaster().broadcast(recv, resource);
					}

					if (logger.isDebugEnabled())
						logger.debug("Sent to resource: {}", resource);

				} catch (ActiveMQException e) {
					logger.error(e, e);
				}
			}
		};
		return handler;
	}

	private void closeSession() {

		if (consumer != null) {
			if (logger.isDebugEnabled())
				logger.debug("Close consumer: {}", tempQ);
			try {

				consumer.close();
				try {
					if (txSession!=null 
							&& txSession.get() != null 
							&& !txSession.get().isClosed()
							&& txSession.get().queueQuery(new SimpleString(getTempQ())).getConsumerCount() == 0) {
						if (logger.isDebugEnabled())
							logger.debug("Delete queue: {}", tempQ);
						txSession.get().deleteQueue(getTempQ());
					}
				} catch (ActiveMQNonExistentQueueException | ActiveMQIllegalStateException e) {
					if (logger.isDebugEnabled())
						logger.debug(e.getMessage());
				}
				
			} catch (ActiveMQException e) {
				logger.warn(e, e);
			}

		}

	}


	public ClientConsumer getConsumer() {

		if (consumer == null && getTxSession() != null && !getTxSession().isClosed()) {
			if (logger.isDebugEnabled())
				logger.debug("Start consumer: {}", getTempQ());
			try {
				try {
					
						getTxSession().createTemporaryQueue("outgoing.reply." + getTempQ(), RoutingType.ANYCAST,
								getTempQ());
				
				} catch (ActiveMQQueueExistsException e) {
					if (logger.isDebugEnabled())
						logger.debug(e.getMessage());
				}
				
				consumer = getTxSession().createConsumer(getTempQ());
				getTxSession().start();
				
			} catch (ActiveMQException e) {
				logger.error(e, e);
			}
		}
		return consumer;

	}

	public String getTempQ() {
		return tempQ;
	}

	protected void addWebsocketCloseListener(AtmosphereResource resource) {
		resource.addEventListener( new WebSocketEventListenerAdapter() {
			
			@Override
			public void onResume(AtmosphereResourceEvent event) {
				if (logger.isDebugEnabled()) 
					logger.debug("onResume: {}",event);
				super.onResume(event);
				try {
					resource.close();
				} catch (IOException e) {
					logger.error(e,e);
				}
				addCloseListener(resource);
		
			}
		});
	}
	
	protected void getPath(String path, Cookie cookie, String time) throws Exception {
		String correlation = getClass().getSimpleName()+"-"+java.util.UUID.randomUUID().toString();
		initSession(correlation);

		path=StringUtils.defaultIfBlank(path,"*");
		if (logger.isDebugEnabled())
			logger.debug("get raw: {}",path);
		path=sanitizeApiPath(path);
		
		if (logger.isDebugEnabled())
			logger.debug("get path: {}",path);
	
		if (logger.isDebugEnabled()) {//
			logger.debug("JwtToken: {}", getToken(cookie));
		}
		if(StringUtils.isNotBlank(time)) {
			sendMessage(getTempQ(),Util.getJsonGetSnapshotRequest(path,getToken(cookie), time).toString(),correlation, getToken(cookie));
		}else {
			sendMessage(getTempQ(),Util.getJsonGetRequest(path,getToken(cookie)).toString(),correlation, getToken(cookie));
		}
		
	}
	
	protected String sanitizeApiPath(String path) {

		path = StringUtils.removeStart(path,SIGNALK_API);
		path = StringUtils.removeStart(path,"/");
		path = path.replace('/', '.');

		
		// handle /vessels.* etc
		path = Util.fixSelfKey(path);
		return path;
	}

	protected String getWebsocket(AtmosphereResource resource, String body, Cookie cookie) {
		try {
			String correlationId = getClass().getSimpleName()+"-stream-" + resource.uuid(); // UUID.randomUUID().toString();

			// resource.suspend();
			
			
			if (logger.isDebugEnabled())
				logger.debug("Correlation: {}, Post: {}", correlationId, body);
			
			initSession(correlationId);
			if(setConsumer(resource, false)) {
				addCloseListener(resource);
				//setConnectionWatcher(resource, PING_PERIOD);
			}

			sendMessage(getTempQ(),addToken(body, cookie), correlationId, getToken(cookie));

		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			try {
				resource.getResponse().sendError(HttpStatus.SC_INTERNAL_SERVER_ERROR);
			} catch (IOException e1) {
				logger.error(e.getMessage(), e);
			}
		}
		return "";
	}

	protected void setConnectionWatcher(AtmosphereResource resource, long period) {
		TimerTask task = new TimerTask() {

			@Override
			public void run() {
				AtmosphereResourceSessionFactory factory = resource.getAtmosphereConfig().framework().sessionFactory();
				AtmosphereResourceSession session = factory.getSession(resource);
				Long lastPing = (Long) session.getAttribute("lastPing");

				if (logger.isDebugEnabled())logger.debug("Get lastPing {}={}", resource.uuid(),lastPing );
				try {
					ping(resource);
					
					if (logger.isDebugEnabled())
						logger.debug("Checking broadcast age < {}", period*3);
					if (lastPing!=null && System.currentTimeMillis() - lastPing > period * 3) {
						
							if (logger.isDebugEnabled())
								logger.debug("Checking ping failed: {} , closing...",
										System.currentTimeMillis() - lastPing);
							resource.close();
							cancel();
							timer.purge();
					}
				}catch (Exception e) {
					logger.debug(e,e);
					cancel();
					timer.purge();
				}
			}

			
		};
		if(logger.isDebugEnabled())
			logger.debug("Created connection watcher");
		timer.schedule(task, period, period);
		if(logger.isDebugEnabled())
			logger.debug("Scheduled connection watcher");
	}

	protected void ping(AtmosphereResource resource) {
		if (logger.isDebugEnabled())
			logger.debug("Sending a ping to {}", resource.uuid());
		// send a ping
			WebSocket ws = resource.getAtmosphereConfig().websocketFactory().find(resource.uuid());
			ws.sendPing("XX".getBytes());
		
	}
	
	protected Json getAppList() throws Exception {
		/*
		 * { 
			   name: ..., , 
			   description: ... ,
			   _location: ..., 
			}
			*/
		Json list = Json.array();
		for (File f : staticDir.listFiles()) {
			if (f.isFile())
				continue;
			if(f.getName().equals("mapcache"))continue;
			if(f.getName().equals("logs"))continue;
			if(f.getName().equals("js"))continue;
			if(f.getName().equals("fonts"))continue;
			if(f.getName().equals("download"))continue;
			if(f.getName().equals("docs"))continue;
			if(f.getName().equals("css"))continue;
			if(f.getName().equals("config"))continue;
			if(f.getName().equals("node_modules"))continue;
			addApp(f,list);

		}
		return list;
	}



	protected void addApp(File f, Json list) throws Exception {
		File p = new File(f, "package.json");
		if (p.exists()) {
			Json pkg = Json.read(FileUtils.readFileToString(p));
			String appName = pkg.at("name").asString();
			String description = pkg.has("description")?pkg.at("description").asString():"";
			String repository = "";
			if(pkg.has("repository")) {
				if(pkg.at("repository").isObject()) {
					repository=pkg.at("repository").at("url").asString();
				}else {
					repository = pkg.at("repository").asString();
				}
			}
			String author = pkg.has("author")&&pkg.at("author").isString()?pkg.at("author").asString():"";
			String version = pkg.has("version")&&pkg.at("version").isString()?pkg.at("version").asString():"";
			String licence = pkg.has("licence")&&pkg.at("licence").isString()?pkg.at("licence").asString():"";
			String name=StringUtils.removePattern(appName, "^.*/");
			name = StringUtils.capitalize(name);
			File i = new File(f, "index.html");
			if(!i.exists()) {
				i = new File(f, "public/index.html");
			}
		
			String s = FileUtils.readFileToString(p);
			//signalk-static/
			String url = i.getPath();
			url=StringUtils.substringAfterLast(url, "signalk-static");
			list.add(Json.object("location",url, "_location",url ,"appName",appName,"name", name,"description",description,"repository",repository,"author",author,"version",version,"licence",licence ));
		}else {
			for (File d : f.listFiles()) {
				if (d.isFile())
					continue;
				addApp(d, list);
			}
		}
		
	}
}
	
	
