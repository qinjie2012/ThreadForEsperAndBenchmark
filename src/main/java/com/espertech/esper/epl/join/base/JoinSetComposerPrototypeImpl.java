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

package com.espertech.esper.epl.join.base;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.EventType;
import com.espertech.esper.core.context.util.AgentInstanceContext;
import com.espertech.esper.core.service.StreamJoinAnalysisResult;
import com.espertech.esper.epl.expression.core.ExprEvaluator;
import com.espertech.esper.epl.expression.core.ExprEvaluatorContext;
import com.espertech.esper.epl.expression.core.ExprNode;
import com.espertech.esper.epl.expression.core.ExprValidationException;
import com.espertech.esper.epl.expression.ops.ExprAndNode;
import com.espertech.esper.epl.expression.ops.ExprAndNodeImpl;
import com.espertech.esper.epl.join.exec.base.ExecNode;
import com.espertech.esper.epl.join.plan.*;
import com.espertech.esper.epl.join.table.EventTable;
import com.espertech.esper.epl.join.table.EventTableUtil;
import com.espertech.esper.epl.join.table.HistoricalStreamIndexList;
import com.espertech.esper.epl.spec.OuterJoinDesc;
import com.espertech.esper.epl.table.mgmt.TableMetadata;
import com.espertech.esper.epl.table.mgmt.TableService;
import com.espertech.esper.epl.table.mgmt.TableStateInstance;
import com.espertech.esper.epl.virtualdw.VirtualDWView;
import com.espertech.esper.epl.virtualdw.VirtualDWViewProviderForAgentInstance;
import com.espertech.esper.view.DerivedValueView;
import com.espertech.esper.view.HistoricalEventViewable;
import com.espertech.esper.view.Viewable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.annotation.Annotation;
import java.util.*;
import java.util.concurrent.locks.Lock;

public class JoinSetComposerPrototypeImpl implements JoinSetComposerPrototype {

    private static final Log log = LogFactory.getLog(JoinSetComposerPrototypeFactory.class);

    private final String statementName;
    private final String statementId;
    private final OuterJoinDesc[] outerJoinDescList;
    private final ExprNode optionalFilterNode;
    private final EventType[] streamTypes;
    private final String[] streamNames;
    private final StreamJoinAnalysisResult streamJoinAnalysisResult;
    private final Annotation[] annotations;
    private final HistoricalViewableDesc historicalViewableDesc;
    private final ExprEvaluatorContext exprEvaluatorContext;
    private final QueryPlanIndex[] indexSpecs;
    private final QueryPlan queryPlan;
    private final HistoricalStreamIndexList[] historicalStreamIndexLists;
    private final boolean joinRemoveStream;
    private final boolean isOuterJoins;
    private final TableService tableService;

    public JoinSetComposerPrototypeImpl(String statementName,
                                        String statementId,
                                        OuterJoinDesc[] outerJoinDescList,
                                        ExprNode optionalFilterNode,
                                        EventType[] streamTypes,
                                        String[] streamNames,
                                        StreamJoinAnalysisResult streamJoinAnalysisResult,
                                        Annotation[] annotations,
                                        HistoricalViewableDesc historicalViewableDesc,
                                        ExprEvaluatorContext exprEvaluatorContext,
                                        QueryPlanIndex[] indexSpecs,
                                        QueryPlan queryPlan,
                                        HistoricalStreamIndexList[] historicalStreamIndexLists,
                                        boolean joinRemoveStream,
                                        boolean isOuterJoins,
                                        TableService tableService) {
        this.statementName = statementName;
        this.statementId = statementId;
        this.outerJoinDescList = outerJoinDescList;
        this.optionalFilterNode = optionalFilterNode;
        this.streamTypes = streamTypes;
        this.streamNames = streamNames;
        this.streamJoinAnalysisResult = streamJoinAnalysisResult;
        this.annotations = annotations;
        this.historicalViewableDesc = historicalViewableDesc;
        this.exprEvaluatorContext = exprEvaluatorContext;
        this.indexSpecs = indexSpecs;
        this.queryPlan = queryPlan;
        this.historicalStreamIndexLists = historicalStreamIndexLists;
        this.joinRemoveStream = joinRemoveStream;
        this.isOuterJoins = isOuterJoins;
        this.tableService = tableService;
    }

    public JoinSetComposerDesc create(Viewable[] streamViews, boolean isFireAndForget, AgentInstanceContext agentInstanceContext) {

        // Build indexes
        Map<TableLookupIndexReqKey, EventTable>[] indexesPerStream = new HashMap[indexSpecs.length];
        Lock[] tableSecondaryIndexLocks = new Lock[indexSpecs.length];
        boolean hasTable = false;
        for (int streamNo = 0; streamNo < indexSpecs.length; streamNo++)
        {
            if (indexSpecs[streamNo] == null) {
                continue;
            }

            Map<TableLookupIndexReqKey, QueryPlanIndexItem> items = indexSpecs[streamNo].getItems();
            indexesPerStream[streamNo] = new LinkedHashMap<TableLookupIndexReqKey, EventTable>();

            if (streamJoinAnalysisResult.getTablesPerStream()[streamNo] != null) {
                // build for tables
                TableMetadata metadata = streamJoinAnalysisResult.getTablesPerStream()[streamNo];
                TableStateInstance state = tableService.getState(metadata.getTableName(), agentInstanceContext.getAgentInstanceId());
                for (String indexName : state.getSecondaryIndexes()) { // add secondary indexes
                    EventTable index = state.getIndex(indexName);
                    indexesPerStream[streamNo].put(new TableLookupIndexReqKey(indexName, metadata.getTableName()), index);
                }
                EventTable index = state.getIndex(metadata.getTableName()); // add primary index
                indexesPerStream[streamNo].put(new TableLookupIndexReqKey(metadata.getTableName(), metadata.getTableName()), index);
                hasTable = true;
                tableSecondaryIndexLocks[streamNo] = agentInstanceContext.getStatementContext().isWritesToTables() ?
                        state.getTableLevelRWLock().writeLock() : state.getTableLevelRWLock().readLock();
            }
            else {
                // build tables for implicit indexes
                for (Map.Entry<TableLookupIndexReqKey, QueryPlanIndexItem> entry : items.entrySet()) {
                    EventTable index;
                    if (streamJoinAnalysisResult.getViewExternal()[streamNo] != null) {
                        VirtualDWView view = streamJoinAnalysisResult.getViewExternal()[streamNo].getView(agentInstanceContext);
                        index = view.getJoinIndexTable(items.get(entry.getKey()));
                    }
                    else {
                        index = EventTableUtil.buildIndex(streamNo, items.get(entry.getKey()), streamTypes[streamNo], false, entry.getValue().isUnique(), null);
                    }
                    indexesPerStream[streamNo].put(entry.getKey(), index);
                }
            }
        }

        // obtain any external views
        VirtualDWViewProviderForAgentInstance[] externalViewProviders = streamJoinAnalysisResult.getViewExternal();
        VirtualDWView[] externalViews = new VirtualDWView[externalViewProviders.length];
        for (int i = 0; i < externalViews.length; i++) {
            if (externalViewProviders[i] != null) {
                externalViews[i] = streamJoinAnalysisResult.getViewExternal()[i].getView(agentInstanceContext);
            }
        }

        // Build strategies
        QueryPlanNode[] queryExecSpecs = queryPlan.getExecNodeSpecs();
        QueryStrategy[] queryStrategies = new QueryStrategy[queryExecSpecs.length];
        for (int i = 0; i < queryExecSpecs.length; i++)
        {
            QueryPlanNode planNode = queryExecSpecs[i];
            if (planNode == null)
            {
                log.debug(".makeComposer No execution node for stream " + i + " '" + streamNames[i] + "'");
                continue;
            }

            ExecNode executionNode = planNode.makeExec(statementName, statementId, annotations, indexesPerStream, streamTypes, streamViews, historicalStreamIndexLists, externalViews, tableSecondaryIndexLocks);

            if (log.isDebugEnabled())
            {
                log.debug(".makeComposer Execution nodes for stream " + i + " '" + streamNames[i] +
                    "' : \n" + ExecNode.print(executionNode));
            }

            queryStrategies[i] = new ExecNodeQueryStrategy(i, streamTypes.length, executionNode);
        }

        // Remove indexes that are from tables as these are only available to query strategies
        if (hasTable) {
            indexesPerStream = removeTableIndexes(indexesPerStream, streamJoinAnalysisResult.getTablesPerStream());
        }

        // If this is not unidirectional and not a self-join (excluding self-outer-join)
        JoinSetComposerDesc joinSetComposerDesc;
        if ((!streamJoinAnalysisResult.isUnidirectional()) &&
            (!streamJoinAnalysisResult.isPureSelfJoin() || outerJoinDescList.length > 0))
        {
            JoinSetComposer composer;
            if (historicalViewableDesc.isHasHistorical())
            {
                composer = new JoinSetComposerHistoricalImpl(indexesPerStream, queryStrategies, streamViews, exprEvaluatorContext);
            }
            else
            {
                if (isFireAndForget) {
                    composer = new JoinSetComposerFAFImpl(indexesPerStream, queryStrategies, streamJoinAnalysisResult.isPureSelfJoin(), exprEvaluatorContext, joinRemoveStream, isOuterJoins);
                }
                else {
                    composer = new JoinSetComposerImpl(indexesPerStream, queryStrategies, streamJoinAnalysisResult.isPureSelfJoin(), exprEvaluatorContext, joinRemoveStream);
                }
            }

            // rewrite the filter expression for all-inner joins in case "on"-clause outer join syntax was used to include those expressions
            ExprNode filterExpression = getFilterExpressionInclOnClause(optionalFilterNode, outerJoinDescList);

            ExprEvaluator postJoinEval = filterExpression == null ? null : filterExpression.getExprEvaluator();
            joinSetComposerDesc = new JoinSetComposerDesc(composer, postJoinEval);
        }
        else
        {
            QueryStrategy driver;
            int unidirectionalStream;
            if (streamJoinAnalysisResult.getUnidirectionalStreamNumber() != -1)
            {
                unidirectionalStream = streamJoinAnalysisResult.getUnidirectionalStreamNumber();
                driver = queryStrategies[unidirectionalStream];
            }
            else
            {
                unidirectionalStream = 0;
                driver = queryStrategies[0];
            }

            JoinSetComposer composer = new JoinSetComposerStreamToWinImpl(indexesPerStream, streamJoinAnalysisResult.isPureSelfJoin(),
                    unidirectionalStream, driver, streamJoinAnalysisResult.getUnidirectionalNonDriving());
            ExprEvaluator postJoinEval = optionalFilterNode == null ? null : optionalFilterNode.getExprEvaluator();
            joinSetComposerDesc = new JoinSetComposerDesc(composer, postJoinEval);
        }

        // compile prior events per stream to preload any indexes
        EventBean[][] eventsPerStream = new EventBean[streamNames.length][];
        ArrayList<EventBean> events = new ArrayList<EventBean>();
        for (int i = 0; i < eventsPerStream.length; i++)
        {
            // For named windows and tables, we don't need to preload indexes from the iterators as this is always done already
            if (streamJoinAnalysisResult.getNamedWindow()[i] || streamJoinAnalysisResult.getTablesPerStream()[i] != null)
            {
                continue;
            }

            Iterator<EventBean> it = null;
            if (!(streamViews[i] instanceof HistoricalEventViewable) && !(streamViews[i] instanceof DerivedValueView))
            {
                try
                {
                    it = streamViews[i].iterator();
                }
                catch (UnsupportedOperationException ex)
                {
                    // Joins do not support the iterator
                }
            }

            if (it != null)
            {
                for (;it.hasNext();)
                {
                    events.add(it.next());
                }
                eventsPerStream[i] = events.toArray(new EventBean[events.size()]);
                events.clear();
            }
            else
            {
                eventsPerStream[i] = new EventBean[0];
            }
        }

        // init
        joinSetComposerDesc.getJoinSetComposer().init(eventsPerStream);

        return joinSetComposerDesc;
    }

    private Map<TableLookupIndexReqKey, EventTable>[] removeTableIndexes(Map<TableLookupIndexReqKey, EventTable>[] indexesPerStream, TableMetadata[] tablesPerStream) {
        Map<TableLookupIndexReqKey, EventTable>[] result = new Map[indexesPerStream.length];
        for (int i = 0; i < indexesPerStream.length; i++) {
            if (tablesPerStream[i] == null) {
                result[i] = indexesPerStream[i];
                continue;
            }
            result[i] = Collections.emptyMap();
        }
        return result;
    }

    private ExprNode getFilterExpressionInclOnClause(ExprNode optionalFilterNode, OuterJoinDesc[] outerJoinDescList)
    {
        if (optionalFilterNode == null) {   // no need to add as query planning is fully based on on-clause
            return null;
        }
        if (outerJoinDescList.length == 0) {  // not an outer-join syntax
            return optionalFilterNode;
        }
        if (!OuterJoinDesc.consistsOfAllInnerJoins(outerJoinDescList)) {    // all-inner joins
            return optionalFilterNode;
        }
        ExprAndNode andNode = new ExprAndNodeImpl();
        andNode.addChildNode(optionalFilterNode);
        for (OuterJoinDesc outerJoinDesc : outerJoinDescList) {
            andNode.addChildNode(outerJoinDesc.makeExprNode(null));
        }
        try {
            andNode.validate(null);
        }
        catch (ExprValidationException ex) {
            throw new RuntimeException("Unexpected exception validating expression: " + ex.getMessage(), ex);
        }
        return andNode;
    }
}
