package org.apache.rya.indexing.pcj.storage.mongo;

import static java.util.Objects.requireNonNull;

import java.util.Set;

import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;

public class MongoPcjDocuments {
    private static final String PCJ_COLLECTION_NAME = "pcjs";
    private static final String VAR_ORDER_FIELD = "var_orders";
    private static final String SPARQL_FIELD = "sparql";
    private static final String PCJ_ID = "_id";

    private final String ryaInstanceName;
    private final MongoClient client;
    private final MongoCollection<Document> pcjCollection;

    public MongoPcjDocuments(final MongoClient client, final String ryaInstanceName) {
        this.client = requireNonNull(client);
        this.ryaInstanceName = requireNonNull(ryaInstanceName);
        pcjCollection = client.getDatabase(ryaInstanceName).getCollection(PCJ_COLLECTION_NAME);
    }

    public void createPcjTable(final String pcjTableName, final Set<VariableOrder> varOrders, final String sparql) {
        final Document pcjDoc = new Document()
            .append(PCJ_ID, pcjTableName)
            .append(SPARQL_FIELD, sparql)
            .append(VAR_ORDER_FIELD, varOrders);
        pcjCollection.insertOne(pcjDoc);
    }
}