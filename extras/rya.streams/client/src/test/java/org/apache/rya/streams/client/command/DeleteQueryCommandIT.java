/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.streams.client.command;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.rya.streams.api.entity.StreamsQuery;
import org.apache.rya.streams.api.queries.InMemoryQueryRepository;
import org.apache.rya.streams.api.queries.QueryChange;
import org.apache.rya.streams.api.queries.QueryChangeLog;
import org.apache.rya.streams.api.queries.QueryRepository;
import org.apache.rya.streams.kafka.KafkaTopics;
import org.apache.rya.streams.kafka.queries.KafkaQueryChangeLog;
import org.apache.rya.streams.kafka.serialization.queries.QueryChangeDeserializer;
import org.apache.rya.streams.kafka.serialization.queries.QueryChangeSerializer;
import org.apache.rya.test.kafka.KafkaTestInstanceRule;
import org.apache.rya.test.kafka.KafkaTestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.util.concurrent.AbstractScheduledService.Scheduler;

/**
 * Integration Test for deleting a query from Rya Streams through a command.
 */
public class DeleteQueryCommandIT {

    private final String ryaInstance = UUID.randomUUID().toString();
    private QueryRepository queryRepo;

    @Rule
    public KafkaTestInstanceRule kafka = new KafkaTestInstanceRule(true);

    @Before
    public void setup() {
        // Make sure the topic that the change log uses exists.
        final String changeLogTopic = KafkaTopics.queryChangeLogTopic(ryaInstance);
        System.out.println("Test Change Log Topic: " + changeLogTopic);
        kafka.createTopic(changeLogTopic);

        // Setup the QueryRepository used by the test.
        final Producer<?, QueryChange> queryProducer = KafkaTestUtil.makeProducer(kafka, StringSerializer.class, QueryChangeSerializer.class);
        final Consumer<?, QueryChange>queryConsumer = KafkaTestUtil.fromStartConsumer(kafka, StringDeserializer.class, QueryChangeDeserializer.class);
        final QueryChangeLog changeLog = new KafkaQueryChangeLog(queryProducer, queryConsumer, changeLogTopic);
        queryRepo = new InMemoryQueryRepository(changeLog, Scheduler.newFixedRateSchedule(0L, 5, TimeUnit.SECONDS));
    }

    @Test
    public void shortParams() throws Exception {
        // Add a few queries to Rya Streams.
        queryRepo.add("query1", true);
        final UUID query2Id = queryRepo.add("query2", false).getQueryId();
        queryRepo.add("query3", true);

        // Show that all three of the queries were added.
        Set<StreamsQuery> queries = queryRepo.list();
        assertEquals(3, queries.size());

        // Delete query 2 using the delete query command.
        final String[] deleteArgs = new String[] {
                "-r", ryaInstance,
                "-i", kafka.getKafkaHostname(),
                "-p", kafka.getKafkaPort(),
                "-q", query2Id.toString()
        };

        final DeleteQueryCommand deleteCommand = new DeleteQueryCommand();
        deleteCommand.execute(deleteArgs);

        // Show query2 was deleted.
        queries = queryRepo.list();
        assertEquals(2, queries.size());

        for(final StreamsQuery query : queries) {
            assertNotEquals(query2Id, query.getQueryId());
        }
    }

    @Test
    public void longParams() throws Exception {
        // Add a few queries to Rya Streams.
        queryRepo.add("query1", true);
        final UUID query2Id = queryRepo.add("query2", false).getQueryId();
        queryRepo.add("query3", true);

        // Show that all three of the queries were added.
        Set<StreamsQuery> queries = queryRepo.list();
        assertEquals(3, queries.size());

        // Delete query 2 using the delete query command.
        final String[] deleteArgs = new String[] {
                "--ryaInstance", "" + ryaInstance,
                "--kafkaHostname", kafka.getKafkaHostname(),
                "--kafkaPort", kafka.getKafkaPort(),
                "--queryID", query2Id.toString()
        };

        final DeleteQueryCommand deleteCommand = new DeleteQueryCommand();
        deleteCommand.execute(deleteArgs);

        // Show query2 was deleted.
        queries = queryRepo.list();
        assertEquals(2, queries.size());

        for(final StreamsQuery query : queries) {
            assertNotEquals(query2Id, query.getQueryId());
        }
    }
}