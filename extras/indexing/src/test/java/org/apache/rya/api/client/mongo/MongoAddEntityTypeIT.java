package org.apache.rya.api.client.mongo;

import org.apache.rya.api.client.AddEntityType;
import org.apache.rya.api.client.AddEntityType.EntityTypeConfiguration;
import org.apache.rya.api.client.Install;
import org.apache.rya.api.client.Install.DuplicateInstanceNameException;
import org.apache.rya.api.client.Install.InstallConfiguration;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.RyaClient;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.mongodb.MongoITBase;
import org.junit.Test;

/**
 * Integration tests the methods of {@link MongoAddEntityType}.
 */
public class MongoAddEntityTypeIT extends MongoITBase {

    @Test(expected = InstanceDoesNotExistException.class)
    public void instanceDoesNotExist() throws Exception {
        final RyaClient ryaClient = MongoRyaClientFactory.build(getConnectionDetails(), getMongoClient());
        // Skip the install step to create error causing situation.
        ryaClient.getAddEntityType().get().addEntityType(conf.getRyaInstanceName(), EntityTypeConfiguration.builder().build());
    }

    @Test
    public void createEntityType() throws Exception {
        final MongoConnectionDetails connectionDetails = getConnectionDetails();
        final RyaClient ryaClient = MongoRyaClientFactory.build(connectionDetails, getMongoClient());
        // Initialize the commands that will be used by this test.
        final AddEntityType addEntityType = ryaClient.getAddEntityType().get();
        final Install installRya = ryaClient.getInstall();
        final InstallConfiguration installConf = InstallConfiguration.builder()
                .setEnableEntityCentricIndex(true)
                .build();
        installRya.install(conf.getRyaInstanceName(), installConf);

        // Create an Entity Type.
        final EntityTypeConfiguration typeConf = EntityTypeConfiguration.builder()
                .setTypeId(new RyaURI("urn:person"))
                .addProperty(new RyaURI("urn:eyeColor"))
                .addProperty(new RyaURI("urn:hairColor"))
                .addProperty(new RyaURI("urn:gender"))
                .addProperty(new RyaURI("urn:age"))
                .build();

        addEntityType.addEntityType(conf.getRyaInstanceName(), typeConf);

        // Verify the RyaDetails were updated to include the new EntityType.


//        final Optional<RyaDetails> ryaDetails = ryaClient.getGetInstanceDetails().getDetails(conf.getRyaInstanceName());
//        final ImmutableMap<String, PCJDetails> details = ryaDetails.get().getPCJIndexDetails().getPCJDetails();
//        final PCJDetails pcjDetails = details.get(pcjId);
//
//        assertEquals(pcjId, pcjDetails.getId());
//        assertFalse( pcjDetails.getLastUpdateTime().isPresent() );
//
//        // Verify the PCJ's metadata was initialized.
//
//        try(final PrecomputedJoinStorage pcjStorage = new MongoPcjStorage(getMongoClient(), conf.getRyaInstanceName())) {
//            final PcjMetadata pcjMetadata = pcjStorage.getPcjMetadata(pcjId);
//            //confirm that the pcj was added to the pcj store.
//            assertEquals(sparql, pcjMetadata.getSparql());
//            assertEquals(0L, pcjMetadata.getCardinality());
//        }
    }

    private MongoConnectionDetails getConnectionDetails() {
        final java.util.Optional<char[]> password = conf.getMongoPassword() != null ?
                java.util.Optional.of(conf.getMongoPassword().toCharArray()) :
                    java.util.Optional.empty();

                return new MongoConnectionDetails(
                        conf.getMongoHostname(),
                        Integer.parseInt(conf.getMongoPort()),
                        java.util.Optional.ofNullable(conf.getMongoUser()),
                        password);
    }

    @Test(expected = RyaClientException.class)
    public void createPCJ_existingType() throws DuplicateInstanceNameException, RyaClientException {
    }
}
