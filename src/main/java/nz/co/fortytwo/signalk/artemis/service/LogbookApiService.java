package nz.co.fortytwo.signalk.artemis.service;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
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
import java.util.*;

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
	    		content = {@Content(
						mediaType = MediaType.APPLICATION_JSON,
						examples = @ExampleObject(name="update", value = "{\"test\"}")
				)
				}),
	    @ApiResponse(responseCode = "500", description = "Internal server error"),
	    @ApiResponse(responseCode = "403", description = "No permission"),
	    @ApiResponse(responseCode = "400", description = "Bad request if message is not understood")
	    })
	@Consumes(MediaType.APPLICATION_JSON)
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Path("event")
	public String postAt(@Parameter(in = ParameterIn.COOKIE, name = SK_TOKEN) @CookieParam(SK_TOKEN) Cookie cookie,
			@Parameter( name="body", 
				description = "A signalk message",
				schema = @Schema(
						example = "{\n" + 
								"  \"value\": {\n" +
								"      \"message\": \"MOB\", \n" +
								"      \"timestamp\": \"2020-06-27T19:01:13.252Z\" \n" +
								"  		}\n" +
								"  }\n" +
								"}")) String body) throws Exception {

			//make a full message now
			String logbookPath="/logbook/event/"+UUID.randomUUID().toString();
			
			Json msg = Util.getJsonPostRequest(sanitizeApiPath(logbookPath),Json.read(body));
			sendMessage(getTempQ(),addToken(msg, cookie),null,getToken(cookie));
			getResource(request).suspend();
			JsonObject ret = new JsonObject();
			ret.addProperty("status", "OKKK");
		System.out.println(ret.toString());
			return ret.toString();
	}

	private JsonArray createJsonObject(List<QueryResult.Series> queryResult) {
		JsonArray measurementsArray = new JsonArray();
		ArrayList<String> columns = (ArrayList<String>) queryResult.get(0).getColumns();
		List<List<Object>> values = queryResult.get(0).getValues();
		int i;
		for(i = 0; i < values.size(); i++) {
			List v = values.get(i);
			System.out.println("values[" + i + "]: " + v);
			JsonObject measurement = new JsonObject();

			measurement.addProperty("time", (String) v.get(columns.indexOf("time")));
			measurement.addProperty("type", (String) v.get(columns.indexOf("type")).toString().replaceAll("_", " "));
			// navigation object include position, stw, sog, cog, heading
			JsonObject navigation = new JsonObject();
			JsonObject long_lat = new JsonObject();
			long_lat.addProperty("long", v.get(columns.indexOf("posLong")).toString());
			long_lat.addProperty("lat", v.get(columns.indexOf("posLat")).toString());
			navigation.add("position", long_lat); // add position to navigation object

			JsonObject heading_value_unit = new JsonObject();
			heading_value_unit.addProperty("value", v.get(columns.indexOf("heading")).toString());
			heading_value_unit.addProperty("unit", "rad");
			navigation.add("heading", heading_value_unit);

			JsonObject stw_value_unit = new JsonObject();
			stw_value_unit.addProperty("value", v.get(columns.indexOf("STW")).toString());
			stw_value_unit.addProperty("unit", "m/s");
			navigation.add("STW", stw_value_unit); // add STW to navigation object

			JsonObject sog_value_unit = new JsonObject();
			sog_value_unit.addProperty("value", v.get(columns.indexOf("SOG")).toString());
			sog_value_unit.addProperty("unit", "m/s");
			navigation.add("SOG", sog_value_unit); // add SOG to navigation object

			JsonObject cog_value_unit = new JsonObject();
			cog_value_unit.addProperty("value", v.get(columns.indexOf("COG")).toString());
			cog_value_unit.addProperty("unit", "rad");
			navigation.add("COG", cog_value_unit); // add COG to navigation object

			measurement.add("navigation", navigation);

			// environment object including depth, wind (aws, awa, tws, twa) water
			JsonObject environment = new JsonObject();
			JsonObject depth_value_unit = new JsonObject();
			depth_value_unit.addProperty("value", v.get(columns.indexOf("depth")).toString());
			depth_value_unit.addProperty("unit", "m"); // add depth to environment object
			environment.add("depth", depth_value_unit);

			JsonObject wind = new JsonObject();
			JsonObject aws_value_unit = new JsonObject();
			aws_value_unit.addProperty("value", v.get(columns.indexOf("AWS")).toString());
			aws_value_unit.addProperty("unit", "m/s");
			wind.add("AWS", aws_value_unit);

			JsonObject awa_value_unit = new JsonObject();
			awa_value_unit.addProperty("value", v.get(columns.indexOf("AWA")).toString());
			awa_value_unit.addProperty("unit", "rad");
			wind.add("AWA", awa_value_unit);

			JsonObject tws_value_unit = new JsonObject();
			tws_value_unit.addProperty("value", v.get(columns.indexOf("TWS")).toString());
			tws_value_unit.addProperty("unit", "m/s");
			wind.add("TWS", tws_value_unit);

			JsonObject twa_value_unit = new JsonObject();
			twa_value_unit.addProperty("value", v.get(columns.indexOf("TWA")).toString());
			twa_value_unit.addProperty("unit", "rad");
			wind.add("TWA", twa_value_unit);

			JsonObject wind_direction_value_unit = new JsonObject();
			wind_direction_value_unit.addProperty("value", v.get(columns.indexOf("windDirection")).toString());
			wind_direction_value_unit.addProperty("unit", "m/s");
			wind.add("windDirection", wind_direction_value_unit);

			JsonObject water = new JsonObject();
			JsonObject temp_value_unit = new JsonObject();
			temp_value_unit.addProperty("value", v.get(columns.indexOf("waterTemp")).toString());
			temp_value_unit.addProperty("unit", "K");
			water.add("temperature", temp_value_unit);

			environment.add("wind", wind);
			environment.add("water", water);
			measurement.add("environment", environment);
			measurementsArray.add(measurement);
		}

		System.out.println("JsonArray: \n" + measurementsArray);
		//return measurement;
		return measurementsArray;
	}

	@Operation(summary = "Request logbook data", description = "Returns the logbook measurements found within the interval \'fromTime\' to  \'toTime\'.")
	@ApiResponses ({
			@ApiResponse(responseCode = "200", description = "OK",
					content = {@Content(mediaType = MediaType.APPLICATION_JSON,
							examples = @ExampleObject(name="update", value = "{\"test\"}"))
					}),
			@ApiResponse(responseCode = "500", description = "Internal server error"),
			@ApiResponse(responseCode = "403", description = "No permission")
	})
	@Produces(MediaType.APPLICATION_JSON)
	@GET
	//@Path("/event/getMeasurements/{path:[^?]*}")
	@Path("/event/getMeasurements/")
	public String getHistory(@Parameter(in = ParameterIn.COOKIE, name = SK_TOKEN) @CookieParam(SK_TOKEN) Cookie cookie,
							 @Parameter( description = "An ISO 8601 format date/time string", example="2020-06-01T00:00:00.067Z" ) @QueryParam("fromTime")String fromTime,
							 @Parameter( description = "An ISO 8601 format date/time string", example="2020-06-26T17:26:25.407Z") @QueryParam("toTime")String toTime) throws Exception
	{
		System.out.println("Here");
		System.out.println("fromTime: " + fromTime);
		System.out.println("toTime: " + toTime);

		// make a check if toTime > fromTime

		String query = "SELECT * FROM event where time >= '" + fromTime + "'and time <= '" + toTime + "' ORDER BY time DESC";
		System.out.println("query: " + query);
		List<QueryResult.Series> queryResult = logbookDbService.getMeasurements(query);
		System.out.println("queryResult: " + queryResult);
		JsonArray measurements;
		if(queryResult == null) {
			System.out.println("HERE");
			measurements = new JsonArray();
		} else {
			measurements = createJsonObject(queryResult);
		}
		System.out.println("-----" + measurements.toString());
		return measurements.toString();
	}

	@Operation(summary = "Request logbook data", description = "Returns the last 'n' logbook entries")
	@ApiResponses ({
			@ApiResponse(responseCode = "200", description = "OK",
					content = {@Content(mediaType = MediaType.APPLICATION_JSON,
							examples = @ExampleObject(name="update", value = "{\"test\"}"))
					}),
			@ApiResponse(responseCode = "500", description = "Internal server error"),
			@ApiResponse(responseCode = "403", description = "No permission")
	})
	@Produces(MediaType.APPLICATION_JSON)
	@GET
	@Path("/event/getMeasurements/amt")
	public String getMeasurementsWithAmt(@Parameter(in = ParameterIn.COOKIE, name = SK_TOKEN) @CookieParam(SK_TOKEN) Cookie cookie,
							 @Parameter( description = "The number of points to return", example="5") @QueryParam("amount")String amount) throws Exception
	{
		System.out.println("Amount: " + amount);

		String query = "SELECT * FROM event ORDER BY time DESC LIMIT " + amount;
		System.out.println("query: " + query);
		List<QueryResult.Series> queryResult = logbookDbService.getMeasurements(query);
		System.out.println("queryResult: " + queryResult);
		JsonArray measurements;
		if(queryResult == null) {
			measurements = new JsonArray();
		} else {
			measurements = createJsonObject(queryResult);
		}
		System.out.println("-----" + measurements.toString());
		return measurements.toString();
	}
}