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
package io.flowwarden.stream.test;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Captures the Logback events of one logger for the duration of a test —
 * for assertions on the level and the presence of a stack trace, not on
 * the wording. Thread-safe: the stream managers log from their own
 * threads while the test polls.
 */
public final class LogCapture implements AutoCloseable {

    private final Logger logger;
    private final RecordingAppender appender = new RecordingAppender();

    private LogCapture(Logger logger) {
        this.logger = logger;
        appender.start();
        logger.addAppender(appender);
    }

    public static LogCapture of(Class<?> type) {
        return new LogCapture((Logger) LoggerFactory.getLogger(type));
    }

    /** Snapshot of the events captured so far, in logging order. */
    public List<ILoggingEvent> events() {
        return List.copyOf(appender.events);
    }

    @Override
    public void close() {
        logger.detachAppender(appender);
        appender.stop();
    }

    private static final class RecordingAppender extends AppenderBase<ILoggingEvent> {
        private final List<ILoggingEvent> events = new CopyOnWriteArrayList<>();

        @Override
        protected void append(ILoggingEvent event) {
            events.add(event);
        }
    }
}
