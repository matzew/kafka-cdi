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
package org.aerogear.kafka.impl;

import org.aerogear.kafka.cdi.annotation.KafkaStream;
import org.aerogear.kafka.serialization.CafdiSerdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.inject.spi.AnnotatedMethod;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

public class DelegationStreamProcessor {

    private final Logger logger = LoggerFactory.getLogger(DelegationStreamProcessor.class);
    final Properties properties = new Properties();
    private AnnotatedMethod annotatedProcessorMethod;
    private KafkaStreams streams;

    public void init(final String bootstrapServers, final AnnotatedMethod annotatedMethod, final BeanManager beanManager) {

        this.annotatedProcessorMethod = annotatedMethod;
        final KafkaStream streamAnnotation = annotatedMethod.getAnnotation(KafkaStream.class);
        final Class<?> keyType = (Class<?>) ((ParameterizedType) annotatedProcessorMethod.getJavaMember().getGenericParameterTypes()[0]).getActualTypeArguments()[0];
        final Class<?> valType = (Class<?>) ((ParameterizedType) annotatedProcessorMethod.getJavaMember().getGenericParameterTypes()[0]).getActualTypeArguments()[1];

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "org-aerogear-kafka-cdi-" + UUID.randomUUID().toString());
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,  CafdiSerdes.serdeFrom(keyType).getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, CafdiSerdes.serdeFrom(valType).getClass());
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 3000L);

        final StreamsConfig cfg = new StreamsConfig(properties);
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream source = builder.stream(streamAnnotation.input());

        // wire method and execute it:
        final Set<Bean<?>> beans = beanManager.getBeans(annotatedProcessorMethod.getJavaMember().getDeclaringClass());
        final Bean<?> propertyResolverBean = beanManager.resolve(beans);
        final CreationalContext<?> creationalContext = beanManager.createCreationalContext(propertyResolverBean);
        final Type processorType = annotatedProcessorMethod.getJavaMember().getDeclaringClass();

        final Object processorInstance = beanManager.getReference(propertyResolverBean, processorType, creationalContext);

        try {
            final Object sink = annotatedProcessorMethod.getJavaMember().invoke(processorInstance, source);

            final Class<?> retKeyType = (Class<?>) ((ParameterizedType) annotatedProcessorMethod.getJavaMember().getGenericReturnType()).getActualTypeArguments()[0];
            final Class<?> retValType = (Class<?>) ((ParameterizedType) annotatedProcessorMethod.getJavaMember().getGenericReturnType()).getActualTypeArguments()[1];

            if (sink instanceof KStream) {

                final KStream streamSink = (KStream) sink;
                streamSink.through(streamAnnotation.output(), Produced.with(CafdiSerdes.serdeFrom(retKeyType), CafdiSerdes.serdeFrom(retValType)));

            } else if (sink instanceof KTable) {

                final KTable tableSink = (KTable) sink;
                tableSink.toStream()
                        .to(streamAnnotation.output(), Produced.with(CafdiSerdes.serdeFrom(retKeyType), CafdiSerdes.serdeFrom(retValType)));
            }

        } catch (IllegalAccessException | InvocationTargetException e) {
            logger.error("error dispatching received value to consumer", e);
        }

        // go!
        try {
            streams = new KafkaStreams(builder.build(), cfg);

            streams.setStateListener((newState, oldState) -> {
                logger.trace("OLD STATE {}", oldState);
                logger.trace("NEW STATE {}", newState);
            });
            logger.trace("Starting the Streaming context");
            streams.start();
        } catch (Exception e) {
            logger.error("Could not start Kafka streaming client", e);
        }
    }
}
