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

import org.aerogear.kafka.cdi.beans.KafkaService;
import org.aerogear.kafka.cdi.beans.mock.MockProvider;
import org.aerogear.kafka.cdi.tests.AbstractTestBase;
import org.aerogear.kafka.cdi.tests.KafkaClusterTestBase;
import org.assertj.core.api.Assertions;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.inject.Inject;

@RunWith(Arquillian.class)
public class ServiceInjectionTest extends KafkaClusterTestBase {

    @Deployment
    public static JavaArchive createDeployment() {

        return AbstractTestBase.createFrameworkDeployment()
                .addPackage(KafkaService.class.getPackage())
                .addPackage(StreamProcessor.class.getPackage())
                .addPackage(MockProvider.class.getPackage());
    }

    @Inject
    private KafkaService service;

    @Test
    public void nonNullProducer() {
        Assertions.assertThat(service.returnProducer()).isNotNull();
    }

}
