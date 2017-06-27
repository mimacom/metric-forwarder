package com.mimacom.metrics;

import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import com.mimacom.metrics.poller.MetricPollerService;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.embedded.LocalServerPort;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.cloud.client.DefaultServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.mock.web.MockServletContext;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


@RunWith(SpringRunner.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@SpringBootTest(classes = {MetricForwarderApplicationTestConfiguration.class, MockServletContext.class}, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class MetricForwarderApplicationTest {

    @MockBean
    private DiscoveryClient discoveryClient;

    @MockBean
    private RestClient esRestClient;

    @LocalServerPort
    private int portOfLocalServer;

    @Autowired
    private MetricPollerService metricPollerService;

    @Test
    public void contextLoads() {
        assertTrue(true);
    }

    @Test
    public void bootstrap() throws Exception {

        Mockito.when(discoveryClient.getServices()).thenReturn(Arrays.asList("TEST-SERVICE1", "TEST-SERVICE2"));
        DefaultServiceInstance localServerInstance = new DefaultServiceInstance("TEST-SERVICE1", "localhost", portOfLocalServer, false);
        Mockito.when(discoveryClient.getInstances("TEST-SERVICE1")).thenReturn(Arrays.asList(localServerInstance, localServerInstance));
        Mockito.when(discoveryClient.getInstances("TEST-SERVICE2")).thenReturn(Arrays.asList(localServerInstance, localServerInstance));

        metricPollerService.pollInstances();

        ArgumentCaptor<HttpEntity> httpEntityCaptor = ArgumentCaptor.forClass(HttpEntity.class);
        ArgumentCaptor<BasicHeader> headerCaptor = ArgumentCaptor.forClass(BasicHeader.class);

        //Verify the bulk was just performed once, and to the proper formatted index
        Mockito.verify(esRestClient,
                Mockito.times(1)).performRequestAsync(Mockito.eq("POST"),
                Mockito.matches("_bulk"),
                Mockito.eq(Collections.emptyMap()),
                httpEntityCaptor.capture(),
                Mockito.any(ResponseListener.class),
                headerCaptor.capture());

        List<HttpEntity> entities = httpEntityCaptor.getAllValues();

        Assert.assertEquals(1, entities.size());

        HttpEntity httpEntity = entities.get(0);

        //get all the sent instructions in the bulk
        List<String> instructions = getInstructions(httpEntity);

        //2 instructions per operation * 2 endpoints * 4 instances equals 16 total instructions
        assertEquals(instructions.size(), 16);

        assertActionAndMetadataInstructions(instructions);

        assertDocumentInstructions(instructions);

        assertHeader(headerCaptor);
    }

    @Test
    public void handleError() throws Exception {
        String exceptionMsgKey = "exceptionMsg.value";
        String exceptionStacktraceKey = "exceptionStacktrace.value";
        String errorKey = "error.value";
        String errorMessage = "Instance not reachable";

        Mockito.when(discoveryClient.getServices()).thenReturn(Arrays.asList("NOT-ACCESSIBLE-TEST-SERVICE1"));
        DefaultServiceInstance localServerInstance = new DefaultServiceInstance("NOT-ACCESSIBLE-TEST-SERVICE1", "localhost", 1, false);
        Mockito.when(discoveryClient.getInstances("NOT-ACCESSIBLE-TEST-SERVICE1")).thenReturn(Arrays.asList(localServerInstance));

        metricPollerService.pollInstances();

        ArgumentCaptor<HttpEntity> entityCaptor = ArgumentCaptor.forClass(HttpEntity.class);
        ArgumentCaptor<BasicHeader> headerCaptor = ArgumentCaptor.forClass(BasicHeader.class);

        Mockito.verify(esRestClient, Mockito.times(1)).performRequestAsync(Mockito.eq("POST"),
                Mockito.matches("_bulk"),
                Mockito.eq(Collections.emptyMap()),
                entityCaptor.capture(),
                Mockito.any(ResponseListener.class),
                headerCaptor.capture());

        List<HttpEntity> entities = entityCaptor.getAllValues();
        Assert.assertEquals(1, entities.size());

        HttpEntity httpEntity = entities.get(0);
        Gson gson = new Gson();

        //get all the sent instructions in the bulk
        List<String> instructions = getInstructions(httpEntity);
        //2 instructions per operation * 2 endpoints * 1 instances equals 4 total instructions
        assertEquals(instructions.size(), 4);

        assertDocumentInstructions(instructions);

        assertActionAndMetadataInstructions(instructions);

        Map<String, String> json = gson.fromJson(instructions.get(1), Map.class);
        Assert.assertEquals(json.get(errorKey), errorMessage);
        Assert.assertTrue(json.get(exceptionMsgKey) != null);
        Assert.assertTrue(json.get(exceptionStacktraceKey) != null);

        json = gson.fromJson(instructions.get(3), Map.class);
        Assert.assertEquals(json.get(errorKey), errorMessage);
        Assert.assertTrue(json.get(exceptionMsgKey) != null);
        Assert.assertTrue(json.get(exceptionStacktraceKey) != null);

        assertHeader(headerCaptor);
    }

    private static List<String> getInstructions(HttpEntity httpEntity) throws Exception {
        return new BufferedReader(new InputStreamReader(httpEntity.getContent())).lines().collect(Collectors.toList());
    }


    private static void assertDocumentInstructions(List<String> instructions) {
        Gson gson = new Gson();
        //get and assert that the document instructions are properly formed
        for (int i = 1; i < instructions.size(); i = i + 2) {
            String instruction = instructions.get(i);
            LinkedTreeMap<String, Object> document = (LinkedTreeMap<String, Object>) gson.fromJson(instruction, Map.class);
            document.forEach((key, value) -> Assert.assertTrue(key.endsWith(".value")));
            assertEnhancedMetadataForEndpoints(document);
        }
    }

    private static void assertActionAndMetadataInstructions(List<String> instructions) {
        Gson gson = new Gson();
        //get and assert the action_and_meta_data instructions are properly formed
        for (int i = 0; i < instructions.size(); i = i + 2) {
            String instruction = instructions.get(i);
            LinkedTreeMap<String, Object> metadataInstruction = (LinkedTreeMap<String, Object>) gson.fromJson(instruction, Map.class);

            Map<String, String> index = (Map<String, String>) metadataInstruction.get("index");
            Assert.assertTrue(index != null);
            Assert.assertTrue(index.get("_type").equals("timestamped-metric"));
            Assert.assertTrue(index.get("_index").matches("microsvcmetrics-metrics-([0-9]{2})") || index.get("_index").matches("microsvcmetrics-health-([0-9]{2})"));

        }
    }

    private static void assertHeader(ArgumentCaptor<BasicHeader> headerCaptor) {
        //get and assert the header (Such header is required as per ES docs)
        Assert.assertTrue(headerCaptor.getAllValues().size() > 0);
        BasicHeader header = headerCaptor.getAllValues().get(0);
        Assert.assertEquals(header.getName(), HttpHeaders.CONTENT_TYPE);
        Assert.assertEquals(header.getValue(), "application/x-ndjson");
    }

    private static void assertEnhancedMetadataForEndpoints(Map<String, Object> json) {
        String metaKeyTimestamp = "timestamp.value";
        String metaKeyHost = "host.value";
        String metaKeyPort = "port.value";
        String metaKeyServiceId = "serviceId.value";
        String endpoint = "endpoint.value";

        Assert.assertTrue(json.get(metaKeyTimestamp) != null);
        Assert.assertTrue(json.get(endpoint).toString().equals("/metrics") || json.get(endpoint).toString().equals("/health"));
        Assert.assertEquals(json.get(metaKeyHost), "localhost");
        Assert.assertTrue(json.get(metaKeyPort) != null);
        Assert.assertTrue(json.get(metaKeyServiceId) != null);
    }
}