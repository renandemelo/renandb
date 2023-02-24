package org.renandb.kvstore.api;


import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;

import org.renandb.kvstore.util.FileUtil;
import org.junit.*;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.http.*;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@SpringBootTest(
        webEnvironment=WebEnvironment.RANDOM_PORT,
        properties = {
                "storage.dir=" + EntryControllerTest.BASE_DIR,
                "storage.cache-size=200",
                "storage.async-processes-active=false" }
)
public class EntryControllerTest
{
    static final String BASE_DIR = "/tmp/sample-db-for-tests";
    //    @MockBean
//    private StorageConfig storageConfig;
    @Autowired
    private TestRestTemplate restTemplate;

    private String baseUrl;
    @LocalServerPort
    int randomServerPort;

    @BeforeClass()
    public static void createBaseDir() throws IOException {
        Path.of(BASE_DIR).toFile().mkdirs();
    }

    @AfterClass()
    public static void deleteBaseDir() throws IOException {
        FileUtil.deleteDir(Path.of(BASE_DIR));
    }

    @Before
    public void prepareURL(){
        this.baseUrl = "http://localhost:"+randomServerPort;
    }

    private String fullUrlFor(String key){
        return this.baseUrl + resourceUrl(key);
    }
    private String resourceUrl(String key){
        if(key == null) return "/entries/";
        return "/entries/" + key;
    }
    @Test
    public void testPutNewEntry() throws URISyntaxException
    {
        Entry entry = new Entry("myKey", "myValue");

        HttpEntity<Entry> request = new HttpEntity<>(entry, new HttpHeaders());

        ResponseEntity<String> result = this.restTemplate.exchange(fullUrlFor("myKey"), HttpMethod.PUT, request, String.class);

        assertEquals(HttpStatus.CREATED, result.getStatusCode());
        assertEquals(resourceUrl("myKey"), result.getHeaders().getLocation().toString());
    }

    @Test
    public void testPostNewEntry()
    {
        Entry entry = new Entry("myKey", "myValue");
        HttpEntity<Entry> request = new HttpEntity<>(entry, new HttpHeaders());
        ResponseEntity<Entry> result = this.restTemplate.exchange(fullUrlFor(null), HttpMethod.POST, request, Entry.class);

        assertEquals(HttpStatus.OK, result.getStatusCode());
        assertEquals(entry.toPair(), result.getBody().toPair());
    }

    @Test
    public void testGetNonExistingEntry()
    {
        ResponseEntity<Entry> result = this.restTemplate.exchange(fullUrlFor("nonExistingKey"), HttpMethod.GET, new HttpEntity<>(null, new HttpHeaders()), Entry.class);

        assertEquals(HttpStatus.NOT_FOUND, result.getStatusCode());
     }

    @Test
    public void testGetExistingEntry()
    {
        Entry entry = new Entry("myKey", "someValue");
        this.restTemplate.exchange(fullUrlFor("myKey"), HttpMethod.PUT, new HttpEntity<>(entry, new HttpHeaders()), Void.class);

        ResponseEntity<Entry> result = this.restTemplate.exchange(fullUrlFor("myKey"), HttpMethod.GET, new HttpEntity<>(null, new HttpHeaders()), Entry.class);

        assertEquals(HttpStatus.OK, result.getStatusCode());
        assertEquals(entry.toPair(), result.getBody().toPair());
    }

    @Test
    public void testDeleteExistingEntryReturnsOK()
    {
        this.restTemplate.exchange(fullUrlFor("myKey"),
            HttpMethod.PUT, new HttpEntity<>(new Entry("myKey", "someValue"),
            new HttpHeaders()), Void.class);

        ResponseEntity<Void> result = this.restTemplate.exchange(fullUrlFor("myKey"), HttpMethod.DELETE, new HttpEntity<>(null, new HttpHeaders()), Void.class);
        assertEquals(HttpStatus.OK, result.getStatusCode());

        result = this.restTemplate.exchange(fullUrlFor("myKey"), HttpMethod.GET,
                new HttpEntity<>(null, new HttpHeaders()), Void.class);
        assertEquals(HttpStatus.NOT_FOUND, result.getStatusCode());
    }

    @Test
    public void testDeleteNonExistingEntryReturnsNoContent()
    {
        ResponseEntity<Void> result = this.restTemplate.exchange(fullUrlFor("myKey"), HttpMethod.DELETE, new HttpEntity<>(null, new HttpHeaders()), Void.class);
        assertEquals(HttpStatus.NO_CONTENT, result.getStatusCode());
    }

}