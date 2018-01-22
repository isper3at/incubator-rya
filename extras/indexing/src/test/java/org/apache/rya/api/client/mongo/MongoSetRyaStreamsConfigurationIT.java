/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.api.client.mongo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Optional;

import org.apache.rya.api.client.Install;
import org.apache.rya.api.client.Install.InstallConfiguration;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.RyaClient;
import org.apache.rya.api.instance.RyaDetails.RyaStreamsDetails;
import org.apache.rya.mongodb.MongoITBase;
import org.junit.Test;

/**
 * Integration tests the methods of {@link MongoSetRyaStreamsConfiguration}.
 */
public class MongoSetRyaStreamsConfigurationIT extends MongoITBase {

    @Test(expected = InstanceDoesNotExistException.class)
    public void instanceDoesNotExist() throws Exception {
        final RyaClient ryaClient = MongoRyaClientFactory.build(getConnectionDetails(), getMongoClient());

        // Skip the install step to create error causing situation.
        final String ryaInstance = conf.getRyaInstanceName();
        final RyaStreamsDetails details = new RyaStreamsDetails("localhost", 6);
        ryaClient.getSetRyaStreamsConfiguration().setRyaStreamsConfiguration(ryaInstance, details);
    }

    @Test
    public void updatesRyaDetails() throws Exception {
        final RyaClient ryaClient = MongoRyaClientFactory.build(getConnectionDetails(), getMongoClient());

        // Install an instance of Rya.
        final String ryaInstance = conf.getRyaInstanceName();
        final Install installRya = ryaClient.getInstall();
        final InstallConfiguration installConf = InstallConfiguration.builder()
                .build();
        installRya.install(ryaInstance, installConf);

        // Fetch its details and show they do not have any RyaStreamsDetails.
        com.google.common.base.Optional<RyaStreamsDetails> streamsDetails =
                ryaClient.getGetInstanceDetails().getDetails(ryaInstance).get().getRyaStreamsDetails();
        assertFalse(streamsDetails.isPresent());

        // Set the details.
        final RyaStreamsDetails details = new RyaStreamsDetails("localhost", 6);
        ryaClient.getSetRyaStreamsConfiguration().setRyaStreamsConfiguration(ryaInstance, details);

        // Fetch its details again and show that they are now filled in.
        streamsDetails = ryaClient.getGetInstanceDetails().getDetails(ryaInstance).get().getRyaStreamsDetails();
        assertEquals(details, streamsDetails.get());
    }

    private MongoConnectionDetails getConnectionDetails() {
        final Optional<char[]> password = conf.getMongoPassword() != null ?
                Optional.of(conf.getMongoPassword().toCharArray()) :
                Optional.empty();

        return new MongoConnectionDetails(
                conf.getMongoHostname(),
                Integer.parseInt(conf.getMongoPort()),
                Optional.ofNullable(conf.getMongoUser()),
                password);
    }
}