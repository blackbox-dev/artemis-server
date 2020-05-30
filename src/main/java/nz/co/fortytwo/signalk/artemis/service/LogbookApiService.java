package nz.co.fortytwo.signalk.artemis.service;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import mjson.Json;
import nz.co.fortytwo.signalk.artemis.tdb.LogbookDbService;
import nz.co.fortytwo.signalk.artemis.util.Util;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.atmosphere.client.TrackMessageSizeInterceptor;
import org.atmosphere.config.service.AtmosphereService;
import org.atmosphere.interceptor.AtmosphereResourceLifecycleInterceptor;
import org.influxdb.dto.QueryResult;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.MediaType;
import java.util.List;
import java.util.UUID;

import static nz.co.fortytwo.signalk.artemis.util.SignalKConstants.SK_TOKEN;

@AtmosphereService(
	dispatch = true,
	interceptors = {AtmosphereResourceLifecycleInterceptor.class, TrackMessageSizeInterceptor.class},
	path = "/logbook",
	servlet = "org.glassfish.jersey.servlet.ServletContainer")
@Path("/logbook/")
@Tag(name = "Logbook")
public class LogbookApiService extends BaseApiService {

	private static Logger logger = LogManager.getLogger(LogbookApiService.class);
	private LogbookDbService logbookDbService;

	@Context
	private HttpServletRequest request;

	public LogbookApiService() {
		this.logbookDbService = new LogbookDbService();
	}

	@Override
	protected void initSession(String tempQ) throws Exception {
		try{
			super.initSession(tempQ);
			super.setConsumer(getResource(request), true);
			addWebsocketCloseListener(getResource(request));
		}catch(Exception e){
			logger.error(e,e);
			throw e;
		}
	}

	@Operation(summary = "Logbook Event", description = " Creates a new logbook event. "
			+ "Creates a uuid and attaches the posted object at the path/uuid/, then returns the uuid."
			+ " This is a 'fire-and-forget' method, see PUT ")
	@ApiResponses ({
	    @ApiResponse(responseCode = "200", description = "OK", 
	    		content = @Content(
                        mediaType = MediaType.TEXT_PLAIN, 
                        schema = @Schema(example = "\"logbook.event.urn:mrn:signalk:uuid:a8fb07c0-1ffd-4663-899c-f16c2baf8270\"")
                        )
                ),
	    @ApiResponse(responseCode = "500", description = "Internal server error"),
	    @ApiResponse(responseCode = "403", description = "No permission"),
	    @ApiResponse(responseCode = "400", description = "Bad request if message is not understood")
	    })
	@Consumes(MediaType.APPLICATION_JSON)
	@POST
	@Path("event")
	public String postAt(@Parameter(in = ParameterIn.COOKIE, name = SK_TOKEN) @CookieParam(SK_TOKEN) Cookie cookie,
			@Parameter( name="body", 
				description = "A signalk message",
				schema = @Schema(
						example = "{\n" + 
								"  \"value\": {\n" +
								"      \"message\": \"MOB\" \n" +
								"  		}\n" +
								"  }\n" +
								"}")) String body) throws Exception {

			//make a full message now
			String logbookPath="/logbook/event/"+UUID.randomUUID().toString();
			
			Json msg = Util.getJsonPostRequest(sanitizeApiPath(logbookPath),Json.read(body));
			sendMessage(getTempQ(),addToken(msg, cookie),null,getToken(cookie));
			getResource(request).suspend();
			return "";
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("event/getMeasurements")
	public List<QueryResult.Series> getMeasurements() throws Exception {
		//String path = req.getPathInfo();
		if (logger.isDebugEnabled())
			logger.debug("get :{} ","self");
		List<QueryResult.Series> queryResult = logbookDbService.getMeasurements();
		return queryResult;
	}
}