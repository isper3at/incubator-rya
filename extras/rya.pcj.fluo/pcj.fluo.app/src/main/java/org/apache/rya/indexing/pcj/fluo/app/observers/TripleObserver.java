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
package org.apache.rya.indexing.pcj.fluo.app.observers;

import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.DELIM;
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.NODEID_BS_DELIM;
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.SP_PREFIX;
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.VAR_DELIM;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.rya.indexing.pcj.fluo.app.IncUpdateDAO;
import org.apache.rya.indexing.pcj.fluo.app.StringTypeLayer;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryMetadataDAO;
import org.apache.rya.indexing.pcj.fluo.app.query.StatementPatternMetadata;

import com.google.common.collect.Maps;

import io.fluo.api.config.ScannerConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.data.Span;
import io.fluo.api.iterator.ColumnIterator;
import io.fluo.api.iterator.RowIterator;
import io.fluo.api.types.TypedObserver;
import io.fluo.api.types.TypedTransactionBase;

/**
 * An observer that matches new Triples to the Statement Patterns that are part
 * of any PCJ that is being maintained. If the triple matches a pattern, then
 * the new result is stored as a binding set for the pattern.
 */
public class TripleObserver extends TypedObserver {

    private final FluoQueryMetadataDAO queryDao = new FluoQueryMetadataDAO();

    public TripleObserver() {
        super(new StringTypeLayer());
    }

    @Override
    public ObservedColumn getObservedColumn() {
        return new ObservedColumn(FluoQueryColumns.TRIPLES, NotificationType.STRONG);
    }

    @Override
    public void process(final TypedTransactionBase tx, final Bytes row, final Column column) {
        //get string representation of triple
        final String triple = IncUpdateDAO.getTripleString(row);

        //get variable metadata for all SP in table
        final ScannerConfiguration sc1 = new ScannerConfiguration();
        sc1.fetchColumn(FluoQueryColumns.STATEMENT_PATTERN_VARIABLE_ORDER.getFamily(), FluoQueryColumns.STATEMENT_PATTERN_VARIABLE_ORDER.getQualifier());
        sc1.setSpan(Span.prefix(SP_PREFIX));

        //see if triple matches conditions of any of the SP
        final RowIterator ri = tx.get(sc1);

        while(ri.hasNext()) {

            final Entry<Bytes, ColumnIterator> next = ri.next();
            final ColumnIterator ci = next.getValue();
            final String spID = next.getKey().toString();

            final StatementPatternMetadata spMetadata = queryDao.readStatementPatternMetadata(tx, spID);
            final String pattern = spMetadata.getStatementPattern();

            while(ci.hasNext()) {
                final String varOrders = ci.next().getValue().toString();
                final String bindingSet = getBindingSet(triple, pattern, varOrders);
                if(bindingSet.length() != 0) {
                    tx.mutate().row(spID + NODEID_BS_DELIM + bindingSet).col(FluoQueryColumns.STATEMENT_PATTERN_BINDING_SET).set(bindingSet);
                }
            }
        }

        // Once the triple has been handled, it may be deleted.
        tx.delete(row, column);
    }

    //determines whether triple matches SPID conditions and generates bindingset
    //whose order is determined by varOrder
    private static String getBindingSet(final String triple, final String spID, final String varOrder) {
        final String[] spIdArray = spID.split(DELIM);
        final String[] tripleArray = triple.split(DELIM);
        final String[] varOrderArray = varOrder.split(VAR_DELIM);
        final Map<String,String> varMap = Maps.newHashMap();

        if(spIdArray.length != 3 || tripleArray.length != 3) {
            throw new IllegalArgumentException("Invald number of components");
        }

        for(int i = 0; i < 3; i ++) {

            if(spIdArray[i].startsWith("-const-")) {
                if(!spIdArray[i].substring(7).equals(tripleArray[i])) {
                    return "";
                }
            } else{
                varMap.put(spIdArray[i], tripleArray[i]);
            }

        }

        String bindingSet = "";

        for (final String element : varOrderArray) {
            if(bindingSet.length() == 0) {
                bindingSet = varMap.get(element);
            } else {
                bindingSet = bindingSet + DELIM + varMap.get(element);
            }
        }

        return bindingSet;
    }
}