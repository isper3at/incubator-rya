/*
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

import java.util.Date;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.Sail;

import com.google.common.base.Optional;

import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import mvm.rya.api.instance.RyaDetails;
import mvm.rya.api.instance.RyaDetails.EntityCentricIndexDetails;
import mvm.rya.api.instance.RyaDetails.FreeTextIndexDetails;
import mvm.rya.api.instance.RyaDetails.GeoIndexDetails;
import mvm.rya.api.instance.RyaDetails.JoinSelectivityDetails;
import mvm.rya.api.instance.RyaDetails.PCJIndexDetails;
import mvm.rya.api.instance.RyaDetails.PCJIndexDetails.FluoDetails;
import mvm.rya.api.instance.RyaDetails.PCJIndexDetails.PCJDetails;
import mvm.rya.api.instance.RyaDetails.PCJIndexDetails.PCJDetails.PCJUpdateStrategy;
import mvm.rya.api.instance.RyaDetails.ProspectorDetails;
import mvm.rya.api.instance.RyaDetails.TemporalIndexDetails;
import mvm.rya.api.instance.RyaDetailsToConfiguration;
import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.indexing.accumulo.ConfigUtils;
import mvm.rya.indexing.accumulo.geo.GeoConstants;
import mvm.rya.sail.config.RyaSailFactory;

public class AccumuloRyaDetailsExample {
    private static final Logger log = Logger.getLogger(AccumuloRyaDetailsExample.class);

    //
    // Connection configuration parameters
    //

    private static final boolean USE_MOCK_INSTANCE = true;
    private static final boolean PRINT_QUERIES = true;
    private static final String INSTANCE = "instance";
    private static final String RYA_TABLE_PREFIX = "x_test_triplestore_";
    private static final String AUTHS = "U";

    public static void main(final String[] args) throws Exception {
        final Configuration conf = getConf();
        conf.setBoolean(ConfigUtils.DISPLAY_QUERY_PLAN, PRINT_QUERIES);

        final RyaDetails.Builder builder = RyaDetails.builder();

        builder.setRyaInstanceName(INSTANCE)
            .setRyaVersion("1.2.3.4")
            .setEntityCentricIndexDetails( new EntityCentricIndexDetails(true) )
            .setGeoIndexDetails( new GeoIndexDetails(false) )
            .setTemporalIndexDetails( new TemporalIndexDetails(true) )
            .setFreeTextDetails( new FreeTextIndexDetails(true) )
            .setPCJIndexDetails(
                    PCJIndexDetails.builder()
                        .setEnabled(true)
                        .setFluoDetails( new FluoDetails("test_instance_rya_pcj_updater") )
                        .addPCJDetails(
                                PCJDetails.builder()
                                    .setId("pcj 1")
                                    .setUpdateStrategy(PCJUpdateStrategy.BATCH)
                                    .setLastUpdateTime( new Date() )
                                    .build())
                        .addPCJDetails(
                                PCJDetails.builder()
                                    .setId("pcj 2")
                                    .setUpdateStrategy(PCJUpdateStrategy.INCREMENTAL)
                                    .build())
                        .build())
            .setProspectorDetails( new ProspectorDetails(Optional.of(new Date())) )
            .setJoinSelectivityDetails( new JoinSelectivityDetails(Optional.of(new Date())) );
        RyaDetailsToConfiguration.addRyaDetailsToConfiguration(builder.build(), conf);

        SailRepository repository = null;
        SailRepositoryConnection conn = null;
        try {
            log.info("Connecting to Indexing Sail Repository.");
            final Sail sail = RyaSailFactory.getInstance(conf);
            repository = new SailRepository(sail);
            repository.initialize();
            conn = repository.getConnection();

            testSecondaryIndexerExists(conn, conf);
        } finally {
            log.info("Shutting down");
            if (repository != null) {
                try {
                    repository.shutDown();
                } catch (final RepositoryException e) { }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (final RepositoryException e) { }
            }
        }
    }

    private static void testSecondaryIndexerExists(final SailRepositoryConnection conn, final Configuration conf) throws RyaDAOException, MalformedQueryException, RepositoryException, TupleQueryResultHandlerException, QueryEvaluationException, AccumuloException, AccumuloSecurityException {
        final Connector accCon = new MockInstance("instance").getConnector("root", "".getBytes());
        Validate.isTrue(!accCon.tableOperations().exists(RYA_TABLE_PREFIX + "geo"));
    }

    private static Configuration getConf() {
        final AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();

        conf.setBoolean(ConfigUtils.USE_MOCK_INSTANCE, USE_MOCK_INSTANCE);
        conf.set(ConfigUtils.USE_PCJ, "true");
        conf.set(ConfigUtils.USE_GEO, "true");
        conf.set(ConfigUtils.USE_FREETEXT, "true");
        conf.set(ConfigUtils.USE_TEMPORAL, "true");
        conf.set(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX,
                RYA_TABLE_PREFIX);
        conf.set(ConfigUtils.CLOUDBASE_USER, "root");
        conf.set(ConfigUtils.CLOUDBASE_PASSWORD, "");
        conf.set(ConfigUtils.CLOUDBASE_INSTANCE, INSTANCE);
        conf.setInt(ConfigUtils.NUM_PARTITIONS, 3);
        conf.set(ConfigUtils.CLOUDBASE_AUTHS, AUTHS);

        // only geo index statements with geo:asWKT predicates
        conf.set(ConfigUtils.GEO_PREDICATES_LIST,
                GeoConstants.GEO_AS_WKT.stringValue());
        return conf;
    }
}
