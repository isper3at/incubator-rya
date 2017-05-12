package org.apache.rya.indexing.pcj.storage.mongo;

import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.apache.rya.mongodb.MockMongoFactory;
import org.bson.Document;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.MongoClient;

public class MongoPcjDocumentsTest {
    private static final String INSTANCE = "TEST";
    private MongoClient client;
    private MongoPcjDocuments docStore;

    @Before
    public void setup() throws Exception {
        client = MockMongoFactory.newFactory().newMongoClient();
        docStore = new MongoPcjDocuments(client, INSTANCE);
    }

    @Test
    public void serialize_createPcjDocument() throws Exception {
        final String sparql = "some sparql";
        final String pcjName = "somePcj";
        final Set<VariableOrder> varOrders = new HashSet<>();
        varOrders.add(new VariableOrder("A", "B", "C"));
        varOrders.add(new VariableOrder("B", "C", "A"));
        varOrders.add(new VariableOrder("C", "A", "B"));
        docStore.createPcjTable(pcjName, varOrders, sparql);

        final Iterable<Document> rez = client.getDatabase(INSTANCE).getCollection("pcjs").find();
        final Iterator<Document> results = rez.iterator();

        final Document expected = new Document()
            .append("_id", "somePcj")
            .append("sparql", "some sparql")
            .append("var_orders", "");


        int count = 0;
        while (results.hasNext()) {
            count++;
            assertEquals(expected, results.next());
        }
        assertEquals(1, count);
    }
}
