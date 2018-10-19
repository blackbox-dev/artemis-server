package nz.co.fortytwo.signalk.artemis.service;

import static nz.co.fortytwo.signalk.artemis.util.SignalKConstants.FORMAT_DELTA;
import static nz.co.fortytwo.signalk.artemis.util.SignalKConstants.POLICY_IDEAL;
import static nz.co.fortytwo.signalk.artemis.util.SignalKConstants.SK_TOKEN;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

import javax.ws.rs.Consumes;
import javax.ws.rs.CookieParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.MediaType;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.atmosphere.annotation.Suspend;
import org.atmosphere.cpr.AtmosphereResource;
import org.atmosphere.cpr.AtmosphereResourceSession;
import org.atmosphere.cpr.AtmosphereResourceSessionFactory;
import org.atmosphere.websocket.WebSocket;
import org.signalk.schema.subscribe.SignalkSubscribe;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import nz.co.fortytwo.signalk.artemis.util.Util;

@Path("/signalk/v1/playback")
@Tag(name = "Websocket Stream API")
public class SignalkPlaybackService extends BaseApiService {

	
	private static Logger logger = LogManager.getLogger(SignalkPlaybackService.class);
	@Context
	private AtmosphereResource resource;

	@Operation(summary = "Request a websocket stream", description = "Submit a Signalk path, startTime and playbackRate to replay history. ")
	@ApiResponses ({
	    @ApiResponse(responseCode = "101", description = "Switching to websocket", 
	    		content = @Content(
                        mediaType = "application/json"                        		
                        )
                ),
	    @ApiResponse(responseCode = "501", description = "History not implemented"),
	    @ApiResponse(responseCode = "500", description = "Internal server error"),
	    @ApiResponse(responseCode = "403", description = "No permission")
	    })
	@Suspend(contentType = MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@GET
	public String getPlaybackWS(@Parameter(in = ParameterIn.COOKIE, name = SK_TOKEN) @CookieParam(SK_TOKEN) Cookie cookie,
			@Parameter( description = "A signalk path", example="/vessel/self/navigation") @QueryParam("subscribe")String subscribe,
			@Parameter( description = "An ISO 8601 format date/time string", example="2015-03-07T12:37:10.523Z") @QueryParam("startTime")String startTime,
			@Parameter( description = "Playback rate multiplier, eg '2' = twice normal speed", example="2") @QueryParam("playbackRate")Double playbackRate) throws Exception {
		
		if (logger.isDebugEnabled())
			logger.debug("get : ws for {}, subscribe={}", resource.getRequest().getRemoteUser(),subscribe);
		if(StringUtils.isBlank(subscribe)|| "all".equals(subscribe)) {
			return getWebsocket(resource, Util.getSubscriptionJson("vessels.self","*",1000,1000,FORMAT_DELTA,POLICY_IDEAL, startTime, playbackRate).toString(),cookie);
		}else{
			return getWebsocket(resource, Util.getSubscriptionJson("vessels.self",subscribe,1000,1000,FORMAT_DELTA,POLICY_IDEAL, startTime, playbackRate).toString(),cookie);
		}
		//return "";
	}

	@Operation(summary = "Request a websocket stream", description = "Post a Signalk SUBSCRIBE message with startTime and playbackRate to replay history.  ")
	@ApiResponses ({
	    @ApiResponse(responseCode = "101", description = "Switching to websocket", 
	    		content = {@Content(mediaType = MediaType.APPLICATION_JSON, 
	    			examples = @ExampleObject(name="update", value = "{\"test\"}"))
	    				}),
	    @ApiResponse(responseCode = "500", description = "Internal server error"),
	    @ApiResponse(responseCode = "403", description = "No permission")
	    })
	@Suspend(contentType = MediaType.APPLICATION_JSON)
	@Consumes(value = MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@POST
	public String postPlayback(@Parameter(in = ParameterIn.COOKIE, name = SK_TOKEN) @CookieParam(SK_TOKEN) Cookie cookie,
			@Parameter( name="body", description = "A signalk SUBSCRIBE message",schema = @Schema(implementation=SignalkSubscribe.class)) String body) {
		
			return getWebsocket(resource, body,cookie);
		
		
	}


	
	

}