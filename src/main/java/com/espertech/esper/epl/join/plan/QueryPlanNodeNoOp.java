/*
 * *************************************************************************************
 *  Copyright (C) 2006-2015 EsperTech, Inc. All rights reserved.                       *
 *  http://www.espertech.com/esper                                                     *
 *  http://www.espertech.com                                                           *
 *  ---------------------------------------------------------------------------------- *
 *  The software in this package is published under the terms of the GPL license       *
 *  a copy of which has been included with this distribution in the license.txt file.  *
 * *************************************************************************************
 */

package com.espertech.esper.epl.join.plan;

import com.espertech.esper.client.EventType;
import com.espertech.esper.epl.join.exec.base.ExecNode;
import com.espertech.esper.epl.join.exec.base.ExecNodeNoOp;
import com.espertech.esper.epl.join.table.EventTable;
import com.espertech.esper.epl.join.table.HistoricalStreamIndexList;
import com.espertech.esper.util.IndentWriter;
import com.espertech.esper.epl.virtualdw.VirtualDWView;
import com.espertech.esper.view.Viewable;

import java.lang.annotation.Annotation;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.locks.Lock;

public class QueryPlanNodeNoOp extends QueryPlanNode {

    private static final ExecNodeNoOp NOOP = new ExecNodeNoOp();

    public ExecNode makeExec(String statementName, String statementId, Annotation[] annotations, Map<TableLookupIndexReqKey, EventTable>[] indexesPerStream, EventType[] streamTypes, Viewable[] streamViews, HistoricalStreamIndexList[] historicalStreamIndexLists, VirtualDWView[] viewExternal, Lock[] tableSecondaryIndexLocks) {
        return NOOP;
    }

    public void addIndexes(HashSet<TableLookupIndexReqKey> usedIndexes) {
    }

    @Override
    protected void print(IndentWriter writer) {
        writer.println("No-Op Execution");
    }
}
