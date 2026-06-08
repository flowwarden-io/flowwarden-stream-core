/*
 * Copyright 2026 FlowWarden
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.flowwarden.stream.actuator;

import io.flowwarden.stream.ChangeStreamContext;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.EnableFlowWarden;
import io.flowwarden.stream.annotation.OnChange;
import io.flowwarden.stream.test.SharedMongoContainer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Duration;

import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(
        classes = FlowWardenActuatorIntegrationTest.TestApp.class,
        properties = {
                "management.endpoints.web.exposure.include=flowwarden,health",
                "management.endpoint.health.show-details=always"
        }
)
@AutoConfigureMockMvc
@ActiveProfiles("test-mvc")
class FlowWardenActuatorIntegrationTest {

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", SharedMongoContainer.MONGO::getReplicaSetUrl);
    }

    @Autowired
    MockMvc mockMvc;

    @Autowired
    io.flowwarden.stream.core.FlowWardenStreamManager streamManager;

    @BeforeEach
    void ensureStreamRunning() {
        if (!streamManager.isRunning("actuator-test-stream")) {
            streamManager.startStream("actuator-test-stream");
        }
        await().atMost(Duration.ofSeconds(10))
                .until(() -> streamManager.isRunning("actuator-test-stream"));
    }

    @Test
    void getFlowWardenEndpointReturnsStreamDetails() throws Exception {
        mockMvc.perform(get("/actuator/flowwarden"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.healthy").value(true))
                .andExpect(jsonPath("$.streams.actuator-test-stream.status").value("UP"))
                .andExpect(jsonPath("$.streams.actuator-test-stream.mode").exists());
    }

    @Test
    void stopStreamChangesStatusToDown() throws Exception {
        mockMvc.perform(post("/actuator/flowwarden/actuator-test-stream/stop"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.action").value("stopped"));

        mockMvc.perform(get("/actuator/flowwarden"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.healthy").value(false))
                .andExpect(jsonPath("$.streams.actuator-test-stream.status").value("DOWN"));
    }

    @Test
    void startStreamAfterStop() throws Exception {
        mockMvc.perform(post("/actuator/flowwarden/actuator-test-stream/stop"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.action").value("stopped"));

        mockMvc.perform(post("/actuator/flowwarden/actuator-test-stream/start"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.action").value("started"));

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() ->
                        mockMvc.perform(get("/actuator/flowwarden"))
                                .andExpect(status().isOk())
                                .andExpect(jsonPath("$.healthy").value(true))
                                .andExpect(jsonPath("$.streams.actuator-test-stream.status").value("UP"))
                );
    }

    @Test
    void restartStreamKeepsItRunning() throws Exception {
        mockMvc.perform(post("/actuator/flowwarden/actuator-test-stream/restart"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.action").value("restarted"));

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() ->
                        mockMvc.perform(get("/actuator/flowwarden"))
                                .andExpect(status().isOk())
                                .andExpect(jsonPath("$.streams.actuator-test-stream.status").value("UP"))
                );
    }

    @Test
    void unsupportedActionReturnsError() throws Exception {
        mockMvc.perform(post("/actuator/flowwarden/actuator-test-stream/pause"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.error").exists())
                .andExpect(jsonPath("$.action").doesNotExist());
    }

    @Test
    void healthEndpointContainsFlowWardenComponent() throws Exception {
        mockMvc.perform(get("/actuator/health"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.components.flowWarden").exists())
                .andExpect(jsonPath("$.components.flowWarden.status").value("UP"))
                .andExpect(jsonPath("$.components.flowWarden.details.streams").value(1))
                .andExpect(jsonPath("$.components.flowWarden.details.healthy").value(1));
    }

    @SpringBootApplication
    @EnableFlowWarden
    @Import(FlowWardenActuatorIntegrationTest.TestStreamHandler.class)
    static class TestApp {
    }

    @ChangeStream(name = "actuator-test-stream", collection = "actuator_test")
    static class TestStreamHandler {
        @OnChange
        void handle(ChangeStreamContext<?> ctx) {
            // no-op for actuator tests
        }
    }
}
