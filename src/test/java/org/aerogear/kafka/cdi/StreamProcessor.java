/**
 * Copyright 2017 Red Hat, Inc, and individual contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.aerogear.kafka.cdi;

import org.aerogear.kafka.cdi.annotation.KafkaStream;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamProcessor {

    Logger logger = LoggerFactory.getLogger(StreamProcessor.class);

    public StreamProcessor() {
        System.out.println("");
    }

    @KafkaStream(input = "input_topic2", output = "output_topic")
    public KStream<String, Long> tableTransformer(final KStream<String, String> source) {

        logger.trace("Initial setup");

        final KStream<String, Long> successCountsPerJob = source.filter((key, value) -> value.equals("Success"))
                .groupByKey()
                .count("successMessagesStore").toStream();

        return successCountsPerJob;
    }
}
