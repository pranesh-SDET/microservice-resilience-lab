package com.resilience.lab.tests;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.http.Fault;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.github.tomakehurst.wiremock.client.WireMock.*;

public class PaymentTemporalFaultInjectionTest {

    private static final String CHARGE_COMMIT_STUB_NAME = "charge-commit";
    private static final Duration CLIENT_TIMEOUT = Duration.ofMillis(1000);

    private WireMockServer wireMockServer;
    private String baseUrl;
    private HttpClient httpClient;
    private Set<String> processedEventIds;
    private Map<String, List<String>> kafkaTopics;

    @BeforeMethod
    public void setUp() {
        wireMockServer = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort());
        wireMockServer.start();
        baseUrl = "http://localhost:" + wireMockServer.port();

        httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(2))
                .build();

        processedEventIds = new HashSet<>();
        kafkaTopics = new HashMap<>();
        kafkaTopics.put("orders-compensated", new ArrayList<>());
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        if (wireMockServer != null) {
            wireMockServer.stop();
        }
    }

    private long countLedgerCommits() {
        List<ServeEvent> events = wireMockServer.getAllServeEvents();
        return events.stream()
                .filter(e -> e.getStubMapping() != null
                        && CHARGE_COMMIT_STUB_NAME.equals(e.getStubMapping().getName()))
                .count();
    }

    private String processOrderEvent(String eventId, String orderId) throws Exception {
        if (!processedEventIds.add(eventId)) {
            return "DUPLICATE_EVENT_IGNORED";
        }

        int maxAttempts = 2;
        boolean paymentSucceeded = false;

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            HttpRequest chargeReq = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + "/payments/charge"))
                    .timeout(CLIENT_TIMEOUT)
                    .header("Content-Type", "application/json")
                    .header("Idempotency-Key", eventId)
                    .POST(HttpRequest.BodyPublishers.ofString("{\"orderId\":\"" + orderId + "\"}"))
                    .build();

            try {
                HttpResponse<String> resp = httpClient.send(chargeReq, HttpResponse.BodyHandlers.ofString());
                if (resp.statusCode() == 200) {
                    paymentSucceeded = true;
                    break;
                }
            } catch (IOException | InterruptedException ex) {
                if (attempt == maxAttempts) {
                    throw ex;
                }
                Thread.sleep(500);
            }
        }

        if (!paymentSucceeded) {
            return "PAYMENT_FAILED";
        }

        HttpRequest inventoryReq = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/inventory/reserve"))
                .timeout(CLIENT_TIMEOUT)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"orderId\":\"" + orderId + "\"}"))
                .build();

        HttpResponse<String> invResp = httpClient.send(inventoryReq, HttpResponse.BodyHandlers.ofString());
        if (invResp.statusCode() == 409 || invResp.statusCode() == 422) {
            HttpRequest refundReq = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + "/payments/refund"))
                    .timeout(CLIENT_TIMEOUT)
                    .header("Idempotency-Key", eventId + "-refund")
                    .POST(HttpRequest.BodyPublishers.ofString("{\"orderId\":\"" + orderId + "\"}"))
                    .build();
            httpClient.send(refundReq, HttpResponse.BodyHandlers.ofString());

            kafkaTopics.get("orders-compensated")
                    .add("{\"eventId\":\"" + eventId + "\",\"orderId\":\"" + orderId + "\",\"reason\":\"INVENTORY_REJECTED\"}");
            return "COMPENSATED";
        }

        return "COMPLETED";
    }

    @Test
    public void testWindow1_FailureBeforeSideEffect_PlainRetrySucceedsOnce() throws Exception {
        String key = "evt-window1-101";

        wireMockServer.stubFor(post(urlEqualTo("/payments/charge"))
                .withName("charge-pre-effect-abort")
                .inScenario("Window1-PreEffect")
                .whenScenarioStateIs(Scenario.STARTED)
                .willReturn(aResponse().withStatus(503))
                .willSetStateTo("READY_FOR_RETRY"));

        wireMockServer.stubFor(post(urlEqualTo("/payments/charge"))
                .withName(CHARGE_COMMIT_STUB_NAME)
                .inScenario("Window1-PreEffect")
                .whenScenarioStateIs("READY_FOR_RETRY")
                .willReturn(aResponse().withStatus(200).withBody("{\"status\":\"CHARGED\"}"))
                .willSetStateTo("CHARGED"));

        wireMockServer.stubFor(post(urlEqualTo("/inventory/reserve"))
                .willReturn(aResponse().withStatus(200)));

        Assert.assertEquals(processOrderEvent(key, "ord-101"), "COMPLETED");
        Assert.assertEquals(countLedgerCommits(), 1L);
        wireMockServer.verify(2, postRequestedFor(urlEqualTo("/payments/charge"))
                .withHeader("Idempotency-Key", equalTo(key)));
    }

    @Test
    public void testWindow2_FailureAfterCommitReplyLost_IdempotentRetryAndConsumerDedup() throws Exception {
        String key = "evt-window2-202";

        wireMockServer.stubFor(post(urlEqualTo("/payments/charge"))
                .withName(CHARGE_COMMIT_STUB_NAME)
                .inScenario("Window2-PostEffect")
                .whenScenarioStateIs(Scenario.STARTED)
                .withHeader("Idempotency-Key", equalTo(key))
                .willReturn(aResponse().withFault(Fault.CONNECTION_RESET_BY_PEER))
                .willSetStateTo("CHARGED"));

        wireMockServer.stubFor(post(urlEqualTo("/payments/charge"))
                .withName("charge-idempotent-replay")
                .inScenario("Window2-PostEffect")
                .whenScenarioStateIs("CHARGED")
                .withHeader("Idempotency-Key", equalTo(key))
                .willReturn(aResponse().withStatus(200).withBody("{\"status\":\"REPLAYED_FROM_LEDGER\"}")));

        wireMockServer.stubFor(post(urlEqualTo("/inventory/reserve"))
                .willReturn(aResponse().withStatus(200)));

        Assert.assertEquals(processOrderEvent(key, "ord-202"), "COMPLETED");
        Assert.assertEquals(processOrderEvent(key, "ord-202"), "DUPLICATE_EVENT_IGNORED");

        wireMockServer.verify(2, postRequestedFor(urlEqualTo("/payments/charge"))
                .withHeader("Idempotency-Key", equalTo(key)));
        Assert.assertEquals(countLedgerCommits(), 1L);
    }

    @Test
    public void testWindow3_DownstreamRejectionAfterCharge_SagaCompensatesWithoutRetryingRejection() throws Exception {
        String key = "evt-window3-303";

        wireMockServer.stubFor(post(urlEqualTo("/payments/charge"))
                .withName(CHARGE_COMMIT_STUB_NAME)
                .inScenario("Window3-Saga")
                .whenScenarioStateIs(Scenario.STARTED)
                .willReturn(aResponse().withStatus(200).withBody("{\"status\":\"CHARGED\"}"))
                .willSetStateTo("CHARGED"));

        wireMockServer.stubFor(post(urlEqualTo("/inventory/reserve"))
                .willReturn(aResponse().withStatus(409).withBody("{\"error\":\"OUT_OF_STOCK\"}")));

        wireMockServer.stubFor(post(urlEqualTo("/payments/refund"))
                .willReturn(aResponse().withStatus(200).withBody("{\"status\":\"REFUNDED\"}")));

        Assert.assertEquals(processOrderEvent(key, "ord-303"), "COMPENSATED");
        Assert.assertEquals(countLedgerCommits(), 1L);
        wireMockServer.verify(1, postRequestedFor(urlEqualTo("/inventory/reserve")));
        wireMockServer.verify(1, postRequestedFor(urlEqualTo("/payments/refund"))
                .withHeader("Idempotency-Key", equalTo(key + "-refund")));
        Assert.assertEquals(kafkaTopics.get("orders-compensated").size(), 1);
    }

    @Test
    public void testNetflixLatencyVariants_UnderAndOverClientTimeout() throws Exception {
        // Variant A: Delay (150ms) under client timeout (400ms) -> No retry
        String underKey = "evt-latency-under-401";
        wireMockServer.stubFor(post(urlEqualTo("/payments/charge"))
                .withName(CHARGE_COMMIT_STUB_NAME)
                .inScenario("Latency-Under")
                .whenScenarioStateIs(Scenario.STARTED)
                .withHeader("Idempotency-Key", equalTo(underKey))
                .willReturn(aResponse().withStatus(200).withFixedDelay(150)));
        wireMockServer.stubFor(post(urlEqualTo("/inventory/reserve"))
                .willReturn(aResponse().withStatus(200)));

        Assert.assertEquals(processOrderEvent(underKey, "ord-401"), "COMPLETED");
        wireMockServer.verify(1, postRequestedFor(urlEqualTo("/payments/charge"))
                .withHeader("Idempotency-Key", equalTo(underKey)));
        Assert.assertEquals(countLedgerCommits(), 1L);

        wireMockServer.resetAll();

        // Variant B: Delay (1400ms) over client timeout (400ms) -> Commits at k=1, times out, retries safely
        String overKey = "evt-latency-over-402";
        wireMockServer.stubFor(post(urlEqualTo("/payments/charge"))
                .withName(CHARGE_COMMIT_STUB_NAME)
                .inScenario("Latency-Over")
                .whenScenarioStateIs(Scenario.STARTED)
                .withHeader("Idempotency-Key", equalTo(overKey))
                .willReturn(aResponse().withStatus(200).withFixedDelay(1400))
                .willSetStateTo("CHARGED"));

        wireMockServer.stubFor(post(urlEqualTo("/payments/charge"))
                .withName("charge-idempotent-replay")
                .inScenario("Latency-Over")
                .whenScenarioStateIs("CHARGED")
                .withHeader("Idempotency-Key", equalTo(overKey))
                .willReturn(aResponse().withStatus(200).withFixedDelay(20)));

        wireMockServer.stubFor(post(urlEqualTo("/inventory/reserve"))
                .willReturn(aResponse().withStatus(200)));

        Assert.assertEquals(processOrderEvent(overKey, "ord-402"), "COMPLETED");
        wireMockServer.verify(2, postRequestedFor(urlEqualTo("/payments/charge"))
                .withHeader("Idempotency-Key", equalTo(overKey)));
        Assert.assertEquals(countLedgerCommits(), 1L);
    }
}