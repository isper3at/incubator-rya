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
package org.apache.rya.indexing.model;

import static org.mockito.Mockito.mock;

import java.util.ArrayList;

import org.apache.rya.indexing.IndexingExpr;
import org.apache.rya.indexing.storage.EventStorage;
import org.junit.Test;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;

/**
 * Unit tests the methods of {@link EventQueryNode}.
 */
public class EventQueryNodeTest {
    @Test(expected = IllegalStateException.class)
    public void constructor_differentSubjects() throws Exception {
        final Var geoSubj = new Var("point");
        final Var geoPred = new Var("-const-http://www.opengis.net/ont/geosparql#asWKT", ValueFactoryImpl.getInstance().createURI("http://www.opengis.net/ont/geosparql#asWKT"));
        final Var geoObj = new Var("wkt");
        final StatementPattern geoSP = new StatementPattern(geoSubj, geoPred, geoObj);

        final Var timeSubj = new Var("time");
        final Var timePred = new Var("-const-http://www.w3.org/2006/time#inXSDDateTime", ValueFactoryImpl.getInstance().createURI("-const-http://www.w3.org/2006/time#inXSDDateTime"));
        final Var timeObj = new Var("time");
        final StatementPattern timeSP = new StatementPattern(timeSubj, timePred, timeObj);
        // This will fail.
        new EventQueryNode(mock(EventStorage.class), geoSP, timeSP, new ArrayList<IndexingExpr>(), new ArrayList<IndexingExpr>());
    }

    @Test(expected = IllegalStateException.class)
    public void constructor_variablePredicate() throws Exception {
        // A pattern that has a variable for its predicate.
        final Var geoSubj = new Var("point");
        final Var geoPred = new Var("geo");
        final Var geoObj = new Var("wkt");
        final StatementPattern geoSP = new StatementPattern(geoSubj, geoPred, geoObj);

        final Var timeSubj = new Var("time");
        final Var timePred = new Var("-const-http://www.w3.org/2006/time#inXSDDateTime", ValueFactoryImpl.getInstance().createURI("-const-http://www.w3.org/2006/time#inXSDDateTime"));
        final Var timeObj = new Var("time");
        final StatementPattern timeSP = new StatementPattern(timeSubj, timePred, timeObj);
        // This will fail.
        new EventQueryNode(mock(EventStorage.class), geoSP, timeSP, new ArrayList<IndexingExpr>(), new ArrayList<IndexingExpr>());
    }
}