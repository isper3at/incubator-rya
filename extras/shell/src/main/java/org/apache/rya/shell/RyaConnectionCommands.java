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
package org.apache.rya.shell;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.nio.CharBuffer;
import java.util.Optional;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.rya.api.client.InstanceExists;
import org.apache.rya.api.client.RyaClient;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.api.client.accumulo.AccumuloConnectionDetails;
import org.apache.rya.api.client.accumulo.AccumuloRyaClientFactory;
import org.apache.rya.api.client.mongo.MongoConnectionDetails;
import org.apache.rya.api.client.mongo.MongoRyaClientFactory;
import org.apache.rya.api.instance.RyaDetails;
import org.apache.rya.api.instance.RyaDetails.RyaStreamsDetails;
import org.apache.rya.shell.SharedShellState.ConnectionState;
import org.apache.rya.shell.SharedShellState.ShellState;
import org.apache.rya.shell.SharedShellState.StorageType;
import org.apache.rya.shell.util.ConnectorFactory;
import org.apache.rya.shell.util.PasswordPrompt;
import org.apache.rya.streams.api.RyaStreamsClient;
import org.apache.rya.streams.kafka.KafkaRyaStreamsClientFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import com.mongodb.MongoClient;
import com.mongodb.MongoException;

/**
 * Spring Shell commands that manage the connection that is used by the shell.
 */
@Component
public class RyaConnectionCommands implements CommandMarker {

    // Command line commands.
    public static final String PRINT_CONNECTION_DETAILS_CMD = "print-connection-details";
    public static final String CONNECT_ACCUMULO_CMD = "connect-accumulo";
    public static final String CONNECT_MONGO_CMD = "connect-mongo";
    public static final String CONNECT_INSTANCE_CMD = "connect-rya";
    public static final String DISCONNECT_COMMAND_NAME_CMD = "disconnect";

    private final SharedShellState sharedState;
    private final PasswordPrompt passwordPrompt;

    /**
     * Constructs an instance of {@link RyaConnectionCommands}.
     *
     * @param state - Holds shared state between all of the command classes. (not null)
     * @param passwordPrompt - Prompts the user for their password when connecting to a Rya store. (not null)
     */
    @Autowired
    public RyaConnectionCommands(final SharedShellState state, final PasswordPrompt passwordPrompt) {
        sharedState = requireNonNull( state );
        this.passwordPrompt = requireNonNull(passwordPrompt);
    }

    @CliAvailabilityIndicator({PRINT_CONNECTION_DETAILS_CMD})
    public boolean isPrintConnectionDetailsAvailable() {
        return true;
    }

    @CliAvailabilityIndicator({CONNECT_ACCUMULO_CMD, CONNECT_MONGO_CMD})
    public boolean areConnectCommandsAvailable() {
        return sharedState.getShellState().getConnectionState() == ConnectionState.DISCONNECTED;
    }

    @CliAvailabilityIndicator({CONNECT_INSTANCE_CMD})
    public boolean isConnectToInstanceAvailable() {
        switch(sharedState.getShellState().getConnectionState()) {
        case CONNECTED_TO_STORAGE:
        case CONNECTED_TO_INSTANCE:
            return true;
        default:
            return false;
        }
    }

    @CliAvailabilityIndicator({DISCONNECT_COMMAND_NAME_CMD})
    public boolean isDisconnectAvailable() {
        return sharedState.getShellState().getConnectionState() != ConnectionState.DISCONNECTED;
    }

    @CliCommand(value = PRINT_CONNECTION_DETAILS_CMD, help = "Print information about the Shell's Rya storage connection.")
    public String printConnectionDetails() {
        // Check to see if the shell is connected to any storages.
        final com.google.common.base.Optional<StorageType> storageType = sharedState.getShellState().getStorageType();
        if(!storageType.isPresent()) {
            return "The shell is not connected to anything.";
        }

        // Create a print out based on what it is connected to.
        switch(storageType.get()) {
        case ACCUMULO:
            final AccumuloConnectionDetails accDetails = sharedState.getShellState().getAccumuloDetails().get();
            return "The shell is connected to an instance of Accumulo using the following parameters:\n" +
            "    Username: " + accDetails.getUsername() + "\n" +
            "    Instance Name: " + accDetails.getInstanceName() + "\n" +
            "    Zookeepers: " + accDetails.getZookeepers();

        case MONGO:
            final MongoConnectionDetails mongoDetails = sharedState.getShellState().getMongoDetails().get();

            final StringBuilder message = new StringBuilder()
                    .append("The shell is connected to an instance of MongoDB using the following parameters:\n")
                    .append("    Hostname: "  + mongoDetails.getHostname() + "\n")
                    .append("    Port: " + mongoDetails.getPort() + "\n");

            if(mongoDetails.getUsername().isPresent()) {
                message.append("    Username: " + mongoDetails.getUsername().get() + "\n");
            }

            return message.toString();

        default:
            throw new RuntimeException("Unrecognized StorageType: " + storageType.get());
        }
    }

    @CliCommand(value = CONNECT_ACCUMULO_CMD, help = "Connect the shell to an instance of Accumulo.")
    public String connectToAccumulo(
            @CliOption(key = {"username"}, mandatory = true, help = "The username that will be used to connect to Accummulo.")
            final String username,
            @CliOption(key = {"instanceName"}, mandatory = true, help = "The name of the Accumulo instance that will be connected to.")
            final String instanceName,
            @CliOption(key = {"zookeepers"}, mandatory = true, help = "A comma delimited list of zookeeper server hostnames.")
            final String zookeepers
            ) {

        try {
            // Prompt the user for their password.
            final char[] password = passwordPrompt.getPassword();
            final Connector connector= new ConnectorFactory().connect(username, CharBuffer.wrap(password), instanceName, zookeepers);

            // Initialize the connected to Accumulo shared state.
            final AccumuloConnectionDetails accumuloDetails = new AccumuloConnectionDetails(username, password, instanceName, zookeepers);
            final RyaClient commands = AccumuloRyaClientFactory.build(accumuloDetails, connector);
            sharedState.connectedToAccumulo(accumuloDetails, commands);

        } catch(IOException | AccumuloException | AccumuloSecurityException e) {
            throw new RuntimeException("Could not connect to Accumulo. Reason: " + e.getMessage(), e);
        }

        return "Connected. You must select a Rya instance to interact with next.";
    }

    @CliCommand(value = CONNECT_MONGO_CMD, help = "Connect the shell to an instance of MongoDB.")
    public String connectToMongo(
            @CliOption(key = {"username"}, mandatory = false, help =
                "The username that will be used to connect to MongoDB when performing administrative tasks.")
            final String username,
            @CliOption(key= {"hostname"}, mandatory = true, help = "The hostname of the MongoDB that will be connected to.")
            final String hostname,
            @CliOption(key= {"port"}, mandatory = true, help = "The port of the MongoDB that will be connected to.")
            final String port) {

        try {
            // If a username was provided, then prompt for a password.
            char[] password = null;
            if(username != null) {
                password = passwordPrompt.getPassword();
            }

            // Create the Mongo Connection Details that describe the Mongo DB Server we are interacting with.
            final MongoConnectionDetails connectionDetails = new MongoConnectionDetails(
                    hostname,
                    Integer.parseInt(port),
                    Optional.ofNullable(username),
                    Optional.ofNullable(password));

            // Connect to a MongoDB server. TODO Figure out how to provide auth info?
            final MongoClient adminClient = new MongoClient(hostname, Integer.parseInt(port));
            // Make sure the client is closed at shutdown.
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    adminClient.close();
                }
            });

            try {
                //attempt to get the connection point, essentially pinging mongo server.
                adminClient.getConnectPoint();
            } catch(final MongoException e) {
            	//had to rethrow to get scope on adminClient.
                adminClient.close();
                throw e;
            }

            // Initialize the connected to Mongo shared state.
            final RyaClient ryaClient = MongoRyaClientFactory.build(connectionDetails, adminClient);
            sharedState.connectedToMongo(connectionDetails, ryaClient);

        } catch (final IOException | MongoException e) {
            throw new RuntimeException("Could not connection to MongoDB. Reason: " + e.getMessage(), e);
        }

        return "Connected. You must select a Rya instance to interact with next.";
    }

    @CliCommand(value = CONNECT_INSTANCE_CMD, help = "Connect to a specific Rya instance")
    public void connectToInstance(
            @CliOption(key = {"instance"}, mandatory = true, help = "The name of the Rya instance the shell will interact with.")
            final String ryaInstance) {
        final RyaClient ryaClient = sharedState.getShellState().getConnectedCommands().get();
        try {
            final InstanceExists instanceExists = ryaClient.getInstanceExists();

            // Make sure the requested instance exists.
            if(!instanceExists.exists(ryaInstance)) {
                throw new RuntimeException(String.format("'%s' does not match an existing Rya instance.", ryaInstance));
            }

            // Store the instance name in the shared state.
            sharedState.connectedToInstance(ryaInstance);

            // If the Rya instance is configured to interact with Rya Streams, then connect the
            // Rya Streams client to the shared state.
            final com.google.common.base.Optional<RyaDetails> ryaDetails = ryaClient.getGetInstanceDetails().getDetails(ryaInstance);
            if(ryaDetails.isPresent()) {
                final com.google.common.base.Optional<RyaStreamsDetails> streamsDetails = ryaDetails.get().getRyaStreamsDetails();
                if(streamsDetails.isPresent()) {
                    final String kafkaHostname = streamsDetails.get().getHostname();
                    final int kafkaPort = streamsDetails.get().getPort();
                    final RyaStreamsClient streamsClient = KafkaRyaStreamsClientFactory.make(ryaInstance, kafkaHostname, kafkaPort);
                    sharedState.connectedToRyaStreams(streamsClient);
                }
            }
        } catch(final RyaClientException e) {
            throw new RuntimeException("Could not connect to Rya instance. Reason: " + e.getMessage(), e);
        }
    }

    @CliCommand(value = DISCONNECT_COMMAND_NAME_CMD, help = "Disconnect the shell's Rya storage connection (Accumulo).")
    public void disconnect() {
        final ShellState shellState = sharedState.getShellState();

        // If connected to Mongo, there is a client that needs to be closed.
        final com.google.common.base.Optional<MongoClient> mongoAdminClient = shellState.getMongoAdminClient();
        if(mongoAdminClient.isPresent()) {
            mongoAdminClient.get().close();
        }

        // If connected to Rya Streams, then close the associated resources.
        final com.google.common.base.Optional<RyaStreamsClient> streamsClient = shellState.getRyaStreamsCommands();
        if(streamsClient.isPresent()) {
            try {
                streamsClient.get().close();
            } catch (final Exception e) {
                System.err.print("Could not close the RyaStreamsClient.");
                e.printStackTrace();
            }
        }

        // Update the shared state to disconnected.
        sharedState.disconnected();
    }
}