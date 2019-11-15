package nz.co.fortytwo.signalk.artemis.tdb;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.junit.Test;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.lightcouch.CouchDbClient;

import static org.junit.jupiter.api.Assertions.*;

class CouchDbServiceTest {
    private static CouchDbClient clientTest;

    @BeforeAll
    static void initAll() {
        clientTest = new CouchDbClient("dbName", true, "http", "127.0.0.1", 5984, null, null);
    }


    @Test
    void saveData() {
        String id = "9435983458";
        boolean found = clientTest.contains(id);
        assertFalse(found);

        JsonObject json = new JsonObject();
        json.addProperty("_id", "doc-id");
        json.add("array", new JsonArray());
        clientTest.save(json);

        found = clientTest.contains("doc-id");
        assertTrue(found);
    }

    @AfterAll
    static void tearDownAll() {
        clientTest.shutdown();
    }

}