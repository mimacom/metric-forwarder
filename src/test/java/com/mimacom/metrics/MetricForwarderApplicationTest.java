package com.mimacom.metrics;

import com.google.gson.Gson;
import org.apache.http.HttpEntity;
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
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;


@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = "eureka.client.enabled=false")
@ActiveProfiles("test")
public class MetricForwarderApplicationTest {

    @MockBean
    private DiscoveryClient discoveryClient;

    @MockBean
    private RestClient restClient;

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

        ArgumentCaptor<HttpEntity> entityCaptor = ArgumentCaptor.forClass(HttpEntity.class);
        Mockito.verify(restClient, Mockito.times(4)).performRequestAsync(Mockito.eq("POST"), Mockito.eq("/microsvcmetrics/metrics"), Mockito.eq(Collections.emptyMap()), entityCaptor.capture(), Mockito.any(ResponseListener.class));

        List<HttpEntity> entities = entityCaptor.getAllValues();
        Assert.assertEquals(4, entities.size());

        HttpEntity httpEntity = entities.get(0);
        Gson gson = new Gson();
        @SuppressWarnings("unchecked")
        Map<String, Object> json = gson.fromJson(new InputStreamReader(httpEntity.getContent()), Map.class);

        json.forEach((key, value) -> Assert.assertTrue(key.endsWith(".value")));

        assertMetadata(json, "TEST-SERVICE1");
    }

    @Test
    public void handleError() throws IOException {
        String exceptionMsgKey = "exceptionMsg.value";
        String exceptionStacktraceKey = "exceptionStacktrace.value";
        String errorKey = "error.value";
        String errorMessage = "Instance not reachable";

        Mockito.when(discoveryClient.getServices()).thenReturn(Arrays.asList("NOT-ACCESSIBLE-TEST-SERVICE1"));
        DefaultServiceInstance localServerInstance = new DefaultServiceInstance("NOT-ACCESSIBLE-TEST-SERVICE1", "localhost", 1, false);
        Mockito.when(discoveryClient.getInstances("NOT-ACCESSIBLE-TEST-SERVICE1")).thenReturn(Arrays.asList(localServerInstance));

        metricPollerService.pollInstances();

        ArgumentCaptor<HttpEntity> entityCaptor = ArgumentCaptor.forClass(HttpEntity.class);
        Mockito.verify(restClient, Mockito.times(1)).performRequestAsync(Mockito.eq("POST"), Mockito.eq("/microsvcmetrics/metrics"), Mockito.eq(Collections.emptyMap()), entityCaptor.capture(), Mockito.any(ResponseListener.class));

        List<HttpEntity> entities = entityCaptor.getAllValues();
        Assert.assertEquals(1, entities.size());

        HttpEntity httpEntity = entities.get(0);
        Gson gson = new Gson();
        @SuppressWarnings("unchecked")
        Map<String, Object> json = gson.fromJson(new InputStreamReader(httpEntity.getContent()), Map.class);
        json.forEach((key, value) -> Assert.assertTrue(key.endsWith(".value")));

        Assert.assertEquals(json.get(errorKey), errorMessage);
        Assert.assertTrue(json.get(exceptionMsgKey) != null);
        Assert.assertTrue(json.get(exceptionStacktraceKey) != null);

        assertMetadata(json, "NOT-ACCESSIBLE-TEST-SERVICE1");

    }

    private static void assertMetadata(Map<String, Object> json, String metaValueServiceId) {
        String metaKeyTimestamp = "timestamp.value";
        String metaKeyHost = "host.value";
        String metaKeyPort = "port.value";
        String metaKeyServiceId = "serviceId.value";

        Assert.assertTrue(json.get(metaKeyTimestamp) != null);
        Assert.assertEquals(json.get(metaKeyHost), "localhost");
        Assert.assertTrue(json.get(metaKeyPort) != null);
        Assert.assertEquals(json.get(metaKeyServiceId), metaValueServiceId);
    }
}