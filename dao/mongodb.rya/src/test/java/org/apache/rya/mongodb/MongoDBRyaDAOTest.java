package org.apache.rya.mongodb;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaStatement.RyaStatementBuilder;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.api.persist.query.RyaQuery;
import org.apache.rya.mongodb.document.visibility.Authorizations;
import org.bson.Document;
import org.calrissian.mango.collect.CloseableIterable;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoDBRyaDAOTest extends MongoRyaTestBase {

    private MongoDBRyaDAO dao;
    private MongoDBRdfConfiguration configuration;

    @Before
    public void setUp() throws IOException, RyaDAOException{
        final Configuration conf = new Configuration();
        conf.set(MongoDBRdfConfiguration.MONGO_DB_NAME, "test");
        conf.set(MongoDBRdfConfiguration.MONGO_COLLECTION_PREFIX, "rya_");
        conf.set(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX, "rya_");
        configuration = new MongoDBRdfConfiguration(conf);
        final int port = mongoClient.getServerAddressList().get(0).getPort();
        configuration.set(MongoDBRdfConfiguration.MONGO_INSTANCE_PORT, ""+port);
        dao = new MongoDBRyaDAO(configuration, mongoClient);
    }


    @Test
    public void testDeleteWildcard() throws RyaDAOException {
        final RyaStatementBuilder builder = new RyaStatementBuilder();
        builder.setPredicate(new RyaURI("http://temp.com"));
        dao.delete(builder.build(), configuration);
    }


    @Test
    public void testAdd() throws RyaDAOException, MongoException, IOException {
        final RyaStatementBuilder builder = new RyaStatementBuilder();
        builder.setPredicate(new RyaURI("http://temp.com"));
        builder.setSubject(new RyaURI("http://subject.com"));
        builder.setObject(new RyaURI("http://object.com"));

        final MongoDatabase db = mongoClient.getDatabase(configuration.get(MongoDBRdfConfiguration.MONGO_DB_NAME));
        final MongoCollection<Document> coll = db.getCollection(configuration.getTriplesCollectionName());

        dao.add(builder.build());

        assertEquals(coll.count(),1);

    }

    @Test
    public void testDelete() throws RyaDAOException, MongoException, IOException {
        final RyaStatementBuilder builder = new RyaStatementBuilder();
        builder.setPredicate(new RyaURI("http://temp.com"));
        builder.setSubject(new RyaURI("http://subject.com"));
        builder.setObject(new RyaURI("http://object.com"));
        final RyaStatement statement = builder.build();

        final MongoDatabase db = mongoClient.getDatabase(configuration.get(MongoDBRdfConfiguration.MONGO_DB_NAME));
        final MongoCollection<Document> coll = db.getCollection(configuration.getTriplesCollectionName());

        dao.add(statement);

        assertEquals(coll.count(),1);

        dao.delete(statement, configuration);

        assertEquals(coll.count(),0);

    }

    @Test
    public void testDeleteWildcardSubjectWithContext() throws RyaDAOException, MongoException, IOException {
        final RyaStatementBuilder builder = new RyaStatementBuilder();
        builder.setPredicate(new RyaURI("http://temp.com"));
        builder.setSubject(new RyaURI("http://subject.com"));
        builder.setObject(new RyaURI("http://object.com"));
        builder.setContext(new RyaURI("http://context.com"));
        final RyaStatement statement = builder.build();

        final MongoDatabase db = mongoClient.getDatabase(configuration.get(MongoDBRdfConfiguration.MONGO_DB_NAME));
        final MongoCollection<Document> coll = db.getCollection(configuration.getTriplesCollectionName());

        dao.add(statement);

        assertEquals(coll.count(),1);

        final RyaStatementBuilder builder2 = new RyaStatementBuilder();
        builder2.setPredicate(new RyaURI("http://temp.com"));
        builder2.setObject(new RyaURI("http://object.com"));
        builder2.setContext(new RyaURI("http://context3.com"));
        final RyaStatement query = builder2.build();

        dao.delete(query, configuration);

        assertEquals(coll.count(),1);

    }

    @Test
    public void testVisibility() throws RyaDAOException, MongoException, IOException {
        // Doc requires "A" and user has "B" = User CANNOT view
        assertFalse(testVisibilityStatement("A", new Authorizations("B")));

        // Doc requires "A" and user has "A" = User can view
        assertTrue(testVisibilityStatement("A", new Authorizations("A")));

        // Doc requires "A" and "B" and user has "A" and "B" = User can view
        assertTrue(testVisibilityStatement("A&B", new Authorizations("A", "B")));

        // Doc requires "A" or "B" and user has "A" and "B" = User can view
        assertTrue(testVisibilityStatement("A|B", new Authorizations("A", "B")));

        // Doc requires "A" and user has "A" and "B" = User can view
        assertTrue(testVisibilityStatement("A", new Authorizations("A", "B")));

        // Doc requires "A" and user has "A" and "B" and "C" = User can view
        assertTrue(testVisibilityStatement("A", new Authorizations("A", "B", "C")));

        // Doc requires "A" and "B" and user has "A" = User CANNOT view
        assertFalse(testVisibilityStatement("A&B", new Authorizations("A")));

        // Doc requires "A" and "B" and "C" and user has "A" and "B" and "C" = User can view
        assertTrue(testVisibilityStatement("A&B&C", new Authorizations("A", "B", "C")));

        // Doc requires "A" and "B" and "C" and user has "A" and "B" = User CANNOT view
        assertFalse(testVisibilityStatement("A&B&C", new Authorizations("A", "B")));

        // Doc requires "A" and "B" and user has "A" and "B" and "C" = User can view
        assertTrue(testVisibilityStatement("A&B", new Authorizations("A", "B", "C")));

        // Doc requires "A" or "B" and user has "A" = User can view
        assertTrue(testVisibilityStatement("A|B", new Authorizations("A")));

        // Doc requires "A" or "B" or "C" and user has "A" and "B" and "C" = User can view
        assertTrue(testVisibilityStatement("A|B|C", new Authorizations("A", "B", "C")));

        // Doc requires "A" or "B" or "C" and user has "A" and "B" = User can view
        assertTrue(testVisibilityStatement("A|B|C", new Authorizations("A", "B")));

        // Doc requires "A" or "B" or "C" and user has "A" = User can view
        assertTrue(testVisibilityStatement("A|B|C", new Authorizations("A")));

        // Doc requires "A" or "B" and user has "A" and "B" and "C" = User can view
        assertTrue(testVisibilityStatement("A|B", new Authorizations("A", "B", "C")));

        // Doc requires "A" and user has "" = User can view
        assertTrue(testVisibilityStatement("A", Authorizations.EMPTY));

        // Doc requires "A" and "B" and user has "" = User can view
        assertTrue(testVisibilityStatement("A&B", Authorizations.EMPTY));

        // Doc requires "A" or "B" and user has "" = User can view
        assertTrue(testVisibilityStatement("A|B", Authorizations.EMPTY));

        // Doc has no requirement and user has "" = User can view
        assertTrue(testVisibilityStatement("", Authorizations.EMPTY));

        // Doc has no requirement and user has "A" = User can view
        assertTrue(testVisibilityStatement("", new Authorizations("A")));

        // Doc has no requirement and user has "A" and "B" = User can view
        assertTrue(testVisibilityStatement("", new Authorizations("A", "B")));

        // Doc requires "A" or ("B" and "C") and user has "A" = User can view
        assertTrue(testVisibilityStatement("A|(B&C)", new Authorizations("A")));

        // Doc requires "A" or ("B" and "C") and user has "B" and "C" = User can view
        assertTrue(testVisibilityStatement("A|(B&C)", new Authorizations("B", "C")));

        // Doc requires "A" and ("B" or "C") and user has "A" and "B" = User can view
        assertTrue(testVisibilityStatement("A&(B|C)", new Authorizations("A", "B")));

        // Doc requires "A" and ("B" or "C") and user has "A" and "B" = User can view
        assertTrue(testVisibilityStatement("A&(B|C)", new Authorizations("A", "C")));

        // Doc requires "(A|B)&(C|(D&E))" and user has "A" and "C" = User can view
        assertTrue(testVisibilityStatement("(A|B)&(C|(D&E))", new Authorizations("A", "C")));

        // Doc requires "(A|B)&(C|(D&E))" and user has "B" and "C" = User can view
        assertTrue(testVisibilityStatement("(A|B)&(C|(D&E))", new Authorizations("B", "C")));

        // Doc requires "(A|B)&(C|(D&E))" and user has "A" and "D" and "E" = User can view
        assertTrue(testVisibilityStatement("(A|B)&(C|(D&E))", new Authorizations("A", "D", "E")));

        // Doc requires "(A|B)&(C|(D&E))" and user has "B" and "D" and "E" = User can view
        assertTrue(testVisibilityStatement("(A|B)&(C|(D&E))", new Authorizations("B", "D", "E")));

        // Doc requires "(A|B)&(C|(D&E))" and user has "B" = User CANNOT view
        assertFalse(testVisibilityStatement("(A|B)&(C|(D&E))", new Authorizations("B")));

        // Doc requires "A|(B&C&(D|E))" and user has "A" = User can view
        assertTrue(testVisibilityStatement("A|(B&C&(D|E))", new Authorizations("A")));

        // Doc requires "A|(B&C&(D|E))" and user has "B" and "C" and "D" = User can view
        assertTrue(testVisibilityStatement("A|(B&C&(D|E))", new Authorizations("B", "C", "D")));

        // Doc requires "A|(B&C&(D|E))" and user has "B" and "C" and "E" = User can view
        assertTrue(testVisibilityStatement("A|(B&C&(D|E))", new Authorizations("B", "C", "E")));

        // Doc requires "A|(B&C&(D|E))" and user has "B" = User CANNOT view
        assertFalse(testVisibilityStatement("A|(B&C&(D|E))", new Authorizations("B")));

        // Doc requires "A|B|C|D|(E&F&G&H)" and user has "A" = User can view
        assertTrue(testVisibilityStatement("A|B|C|D|(E&F&G&H)", new Authorizations("A")));

        // Doc requires "A|B|C|D|(E&F&G&H)" and user has "E" = User CANNOT view
        assertFalse(testVisibilityStatement("A|B|C|D|(E&F&G&H)", new Authorizations("E")));

        // Doc requires "A|B|C|D|(E&F&G&H)" and user has "E" and "F" = User CANNOT view
        assertFalse(testVisibilityStatement("A|B|C|D|(E&F&G&H)", new Authorizations("E", "F")));

        // Doc requires "A|B|C|D|(E&F&G&H)" and user has "I" = User CANNOT view
        assertFalse(testVisibilityStatement("A|B|C|D|(E&F&G&H)", new Authorizations("I")));

        // Doc requires "A|B|C|D|(E&F&G&H)" and user has "A" and "I" = User can view
        assertTrue(testVisibilityStatement("A|B|C|D|(E&F&G&H)", new Authorizations("A", "I")));

        // Doc requires "A|B|C|D|(E&F&G&H)" and user has "E" and "F" and "G" and "H" = User can view
        assertTrue(testVisibilityStatement("A|B|C|D|(E&F&G&H)", new Authorizations("E", "F", "G", "H")));

        // Doc requires "A|B|C|D|(E&F&G&H)" and user has "E" and "F" and "G" and "H" and "I" = User can view
        assertTrue(testVisibilityStatement("A|B|C|D|(E&F&G&H)", new Authorizations("E", "F", "G", "H", "I")));
    }

    /**
     * Generates a test statement with the provided document visibility to
     * determine if the specified user authorization can view the statement.
     * @param documentVisibility the document visibility boolean expression
     * string.
     * @param userAuthorizations the user authorization strings.
     * @return {@code true} if provided authorization could access the document
     * in the collection. {@code false} otherwise.
     * @throws RyaDAOException
     */
    private boolean testVisibilityStatement(final String documentVisibility, final Authorizations userAuthorizations) throws RyaDAOException {
        final MongoDatabase db = mongoClient.getDatabase(configuration.get(MongoDBRdfConfiguration.MONGO_DB_NAME));
        final MongoCollection<Document> coll = db.getCollection(configuration.getTriplesCollectionName());

        final RyaStatement statement = buildVisibilityTestRyaStatement(documentVisibility);

        dao.getConf().setAuths(Authorizations.EMPTY.getAuthorizationsStringArray());
        dao.add(statement);
        dao.getConf().setAuths(userAuthorizations.getAuthorizationsStringArray());

        assertEquals(coll.count(), 1);

        final MongoDBQueryEngine queryEngine = (MongoDBQueryEngine) dao.getQueryEngine();
        queryEngine.setConf(configuration);
        final CloseableIterable<RyaStatement> iter = queryEngine.query(new RyaQuery(statement));

        // Check if user has authorization to view document based on its visibility
        final boolean hasNext = iter.iterator().hasNext();

        // Reset
        dao.delete(statement, configuration);
        assertEquals(coll.count(), 0);
        dao.getConf().setAuths(Authorizations.EMPTY.getAuthorizationsStringArray());

        return hasNext;
    }

    private static RyaStatement buildVisibilityTestRyaStatement(final String documentVisibility) {
        final RyaStatementBuilder builder = new RyaStatementBuilder();
        builder.setPredicate(new RyaURI("http://temp.com"));
        builder.setSubject(new RyaURI("http://subject.com"));
        builder.setObject(new RyaURI("http://object.com"));
        builder.setContext(new RyaURI("http://context.com"));
        builder.setColumnVisibility(documentVisibility.getBytes());
        final RyaStatement statement = builder.build();
        return statement;
    }
}
