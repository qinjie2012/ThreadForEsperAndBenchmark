/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.core.start;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.EventType;
import com.espertech.esper.client.context.ContextPartitionSelector;
import com.espertech.esper.collection.MultiKey;
import com.espertech.esper.collection.Pair;
import com.espertech.esper.collection.UniformPair;
import com.espertech.esper.core.context.mgr.ContextPropertyRegistryImpl;
import com.espertech.esper.core.context.util.AgentInstanceContext;
import com.espertech.esper.core.service.EPPreparedQueryResult;
import com.espertech.esper.core.service.EPServicesContext;
import com.espertech.esper.core.service.StatementContext;
import com.espertech.esper.core.service.StreamJoinAnalysisResult;
import com.espertech.esper.epl.core.*;
import com.espertech.esper.epl.expression.table.ExprTableAccessNode;
import com.espertech.esper.epl.expression.core.ExprNode;
import com.espertech.esper.epl.expression.core.ExprNodeUtility;
import com.espertech.esper.epl.expression.core.ExprValidationException;
import com.espertech.esper.epl.join.base.*;
import com.espertech.esper.epl.spec.NamedWindowConsumerStreamSpec;
import com.espertech.esper.epl.spec.StatementSpecCompiled;
import com.espertech.esper.epl.spec.StreamSpecCompiled;
import com.espertech.esper.epl.spec.TableQueryStreamSpec;
import com.espertech.esper.epl.virtualdw.VirtualDWView;
import com.espertech.esper.epl.virtualdw.VirtualDWViewProviderForAgentInstance;
import com.espertech.esper.event.EventBeanReader;
import com.espertech.esper.event.EventBeanReaderDefaultImpl;
import com.espertech.esper.event.EventBeanUtility;
import com.espertech.esper.event.EventTypeSPI;
import com.espertech.esper.filter.FilterSpecCompiled;
import com.espertech.esper.filter.FilterSpecCompiler;
import com.espertech.esper.util.AuditPath;
import com.espertech.esper.view.Viewable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;

/**
 * Starts and provides the stop method for EPL statements.
 */
public class EPPreparedExecuteMethodQuery implements EPPreparedExecuteMethod
{
    private static final Log queryPlanLog = LogFactory.getLog(AuditPath.QUERYPLAN_LOG);
    private static final Log log = LogFactory.getLog(EPPreparedExecuteMethodQuery.class);

    private final StatementSpecCompiled statementSpec;
    private final ResultSetProcessor resultSetProcessor;
    private final FireAndForgetProcessor[] processors;
    private final AgentInstanceContext agentInstanceContext;
    private final EPServicesContext services;
    private EventBeanReader eventBeanReader;
    private JoinSetComposerPrototype joinSetComposerPrototype;
    private final FilterSpecCompiled[] filters;
    private boolean hasTableAccess;

    /**
     * Ctor.
     * @param statementSpec is a container for the definition of all statement constructs that
     * may have been used in the statement, i.e. if defines the select clauses, insert into, outer joins etc.
     * @param services is the service instances for dependency injection
     * @param statementContext is statement-level information and statement services
     * @throws ExprValidationException if the preparation failed
     */
    public EPPreparedExecuteMethodQuery(StatementSpecCompiled statementSpec,
                                        EPServicesContext services,
                                        StatementContext statementContext)
            throws ExprValidationException
    {
        boolean queryPlanLogging = services.getConfigSnapshot().getEngineDefaults().getLogging().isEnableQueryPlan();
        if (queryPlanLogging) {
            queryPlanLog.info("Query plans for Fire-and-forget query '" + statementContext.getExpression() + "'");
        }

        this.hasTableAccess = (statementSpec.getTableNodes() != null && statementSpec.getTableNodes().length > 0);
        for (StreamSpecCompiled streamSpec : statementSpec.getStreamSpecs()) {
            hasTableAccess |= streamSpec instanceof TableQueryStreamSpec;
        }

        this.statementSpec = statementSpec;
        this.services = services;

        EPPreparedExecuteMethodHelper.validateFAFQuery(statementSpec);

        int numStreams = statementSpec.getStreamSpecs().length;
        EventType[] typesPerStream = new EventType[numStreams];
        String[] namesPerStream = new String[numStreams];
        processors = new FireAndForgetProcessor[numStreams];
        agentInstanceContext = new AgentInstanceContext(statementContext, null, -1, null, null, statementContext.getDefaultAgentInstanceScriptContext());

        // resolve types and processors
        for (int i = 0; i < numStreams; i++) {
            final StreamSpecCompiled streamSpec = statementSpec.getStreamSpecs()[i];
            processors[i] = FireAndForgetProcessorFactory.validateResolveProcessor(streamSpec, services);

            String streamName = processors[i].getNamedWindowOrTableName();
            if (streamSpec.getOptionalStreamName() != null) {
                streamName = streamSpec.getOptionalStreamName();
            }
            namesPerStream[i] = streamName;
            typesPerStream[i] = processors[i].getEventTypeResultSetProcessor();
        }

        // compile filter to optimize access to named window
        filters = new FilterSpecCompiled[numStreams];
        if (statementSpec.getFilterRootNode() != null) {
            LinkedHashMap<String, Pair<EventType, String>> tagged = new LinkedHashMap<String, Pair<EventType, String>>();
            for (int i = 0; i < numStreams; i++) {
                try {
                    StreamTypeServiceImpl types = new StreamTypeServiceImpl(typesPerStream, namesPerStream, new boolean[numStreams], services.getEngineURI(), false);
                    filters[i] = FilterSpecCompiler.makeFilterSpec(typesPerStream[i], namesPerStream[i],
                            Collections.singletonList(statementSpec.getFilterRootNode()), null,
                            tagged, tagged, types,
                            null, statementContext, Collections.singleton(i));
                }
                catch (Exception ex) {
                    log.warn("Unexpected exception analyzing filter paths: " + ex.getMessage(), ex);
                }
            }
        }

        // obtain result set processor
        boolean[] isIStreamOnly = new boolean[namesPerStream.length];
        Arrays.fill(isIStreamOnly, true);
        StreamTypeService typeService = new StreamTypeServiceImpl(typesPerStream, namesPerStream, isIStreamOnly, services.getEngineURI(), true);
        EPStatementStartMethodHelperValidate.validateNodes(statementSpec, statementContext, typeService, null);

        ResultSetProcessorFactoryDesc resultSetProcessorPrototype = ResultSetProcessorFactoryFactory.getProcessorPrototype(statementSpec, statementContext, typeService, null, new boolean[0], true, ContextPropertyRegistryImpl.EMPTY_REGISTRY, null, services.getConfigSnapshot());
        resultSetProcessor = EPStatementStartMethodHelperAssignExpr.getAssignResultSetProcessor(agentInstanceContext, resultSetProcessorPrototype);

        if (statementSpec.getSelectClauseSpec().isDistinct())
        {
            if (resultSetProcessor.getResultEventType() instanceof EventTypeSPI) {
                eventBeanReader = ((EventTypeSPI) resultSetProcessor.getResultEventType()).getReader();
            }
            if (eventBeanReader == null) {
                eventBeanReader = new EventBeanReaderDefaultImpl(resultSetProcessor.getResultEventType());
            }
        }

        // plan joins or simple queries
        if (numStreams > 1)
        {
            StreamJoinAnalysisResult streamJoinAnalysisResult = new StreamJoinAnalysisResult(numStreams);
            Arrays.fill(streamJoinAnalysisResult.getNamedWindow(), true);
            for (int i = 0; i < numStreams; i++) {
                final FireAndForgetInstance processorInstance = processors[i].getProcessorInstance(agentInstanceContext);
                if (processors[i].isVirtualDataWindow()) {
                    streamJoinAnalysisResult.getViewExternal()[i] = new VirtualDWViewProviderForAgentInstance() {
                        public VirtualDWView getView(AgentInstanceContext agentInstanceContext) {
                            return processorInstance.getVirtualDataWindow();
                        }
                    };
                }
                String[][] uniqueIndexes = processors[i].getUniqueIndexes(processorInstance);
                streamJoinAnalysisResult.getUniqueKeys()[i] = uniqueIndexes;
            }

            boolean hasAggregations = !resultSetProcessorPrototype.getAggregationServiceFactoryDesc().getExpressions().isEmpty();
            joinSetComposerPrototype = JoinSetComposerPrototypeFactory.makeComposerPrototype(null, null,
                    statementSpec.getOuterJoinDescList(), statementSpec.getFilterRootNode(), typesPerStream, namesPerStream,
                    streamJoinAnalysisResult, queryPlanLogging, statementContext, new HistoricalViewableDesc(numStreams), agentInstanceContext, false, hasAggregations, services.getTableService(), true);
        }

        // check context partition use
        if (statementSpec.getOptionalContextName() != null) {
            if (numStreams > 1) {
                throw new ExprValidationException("Joins in runtime queries for context partitions are not supported");
            }
        }
    }

    /**
     * Returns the event type of the prepared statement.
     * @return event type
     */
    public EventType getEventType()
    {
        return resultSetProcessor.getResultEventType();
    }

    /**
     * Executes the prepared query.
     * @return query results
     */
    public EPPreparedQueryResult execute(ContextPartitionSelector[] contextPartitionSelectors)
    {
        try {
            int numStreams = processors.length;

            if (contextPartitionSelectors != null && contextPartitionSelectors.length != numStreams) {
                throw new IllegalArgumentException("Number of context partition selectors does not match the number of named windows in the from-clause");
            }

            // handle non-context case
            if (statementSpec.getOptionalContextName() == null) {

                Collection<EventBean>[] snapshots = new Collection[numStreams];
                for (int i = 0; i < numStreams; i++) {

                    ContextPartitionSelector selector = contextPartitionSelectors == null ? null : contextPartitionSelectors[i];
                    snapshots[i] = getStreamFilterSnapshot(i, selector);
                }

                resultSetProcessor.clear();
                return process(snapshots);
            }

            List<ContextPartitionResult> contextPartitionResults = new ArrayList<ContextPartitionResult>();
            ContextPartitionSelector singleSelector = contextPartitionSelectors != null && contextPartitionSelectors.length > 0 ? contextPartitionSelectors[0] : null;

            // context partition runtime query
            Collection<Integer> agentInstanceIds = EPPreparedExecuteMethodHelper.getAgentInstanceIds(processors[0], singleSelector, services.getContextManagementService(), statementSpec.getOptionalContextName());

            // collect events and agent instances
            for (int agentInstanceId : agentInstanceIds) {
                FireAndForgetInstance processorInstance = processors[0].getProcessorInstanceContextById(agentInstanceId);
                if (processorInstance != null) {
                    EPPreparedExecuteTableHelper.assignTableAccessStrategies(services, statementSpec.getTableNodes(), processorInstance.getAgentInstanceContext());
                    Collection<EventBean> coll = processorInstance.snapshotBestEffort(this, filters[0], statementSpec.getAnnotations());
                    contextPartitionResults.add(new ContextPartitionResult(coll, processorInstance.getAgentInstanceContext()));
                }
            }

            // process context partitions
            ArrayDeque<EventBean[]> events = new ArrayDeque<EventBean[]>();
            for (ContextPartitionResult contextPartitionResult : contextPartitionResults) {
                Collection<EventBean> snapshot = contextPartitionResult.getEvents();
                if (statementSpec.getFilterRootNode() != null) {
                    snapshot = getFiltered(snapshot, Collections.singletonList(statementSpec.getFilterRootNode()));
                }
                EventBean[] rows = snapshot.toArray(new EventBean[snapshot.size()]);
                resultSetProcessor.setAgentInstanceContext(contextPartitionResult.getContext());
                UniformPair<EventBean[]> results = resultSetProcessor.processViewResult(rows, null, true);
                if (results != null && results.getFirst() != null && results.getFirst().length > 0) {
                    events.add(results.getFirst());
                }
            }
            return new EPPreparedQueryResult(resultSetProcessor.getResultEventType(), EventBeanUtility.flatten(events));
        }
        finally {
            if (hasTableAccess) {
                services.getTableService().getTableExprEvaluatorContext().releaseAcquiredLocks();
            }
        }
    }

    private Collection<EventBean> getStreamFilterSnapshot(int streamNum, ContextPartitionSelector contextPartitionSelector) {
        final StreamSpecCompiled streamSpec = statementSpec.getStreamSpecs()[streamNum];
        List<ExprNode> filterExpressions = Collections.emptyList();
        if (streamSpec instanceof NamedWindowConsumerStreamSpec) {
            NamedWindowConsumerStreamSpec namedSpec = (NamedWindowConsumerStreamSpec) streamSpec;
            filterExpressions = namedSpec.getFilterExpressions();
        }
        else {
            TableQueryStreamSpec tableSpec = (TableQueryStreamSpec) streamSpec;
            filterExpressions = tableSpec.getFilterExpressions();
        }

        FireAndForgetProcessor fireAndForgetProcessor = processors[streamNum];

        // handle the case of a single or matching agent instance
        FireAndForgetInstance processorInstance = fireAndForgetProcessor.getProcessorInstance(agentInstanceContext);
        if (processorInstance != null) {
            EPPreparedExecuteTableHelper.assignTableAccessStrategies(services, statementSpec.getTableNodes(), agentInstanceContext);
            return getStreamSnapshotInstance(streamNum, filterExpressions, processorInstance);
        }

        // context partition runtime query
        Collection<Integer> contextPartitions = EPPreparedExecuteMethodHelper.getAgentInstanceIds(fireAndForgetProcessor, contextPartitionSelector, services.getContextManagementService(), fireAndForgetProcessor.getContextName());

        // collect events
        ArrayDeque<EventBean> events = new ArrayDeque<EventBean>();
        for (int agentInstanceId : contextPartitions) {
            processorInstance = fireAndForgetProcessor.getProcessorInstanceContextById(agentInstanceId);
            if (processorInstance != null) {
                Collection<EventBean> coll = processorInstance.snapshotBestEffort(this, filters[streamNum], statementSpec.getAnnotations());
                events.addAll(coll);
            }
        }
        return events;
    }

    private Collection<EventBean> getStreamSnapshotInstance(int streamNum, List<ExprNode> filterExpressions, FireAndForgetInstance processorInstance) {
        Collection<EventBean> coll = processorInstance.snapshotBestEffort(this, filters[streamNum], statementSpec.getAnnotations());
        if (filterExpressions.size() != 0) {
            coll = getFiltered(coll, filterExpressions);
        }
        return coll;
    }

    private EPPreparedQueryResult process(Collection<EventBean>[] snapshots) {

        int numStreams = processors.length;

        UniformPair<EventBean[]> results;
        if (numStreams == 1)
        {
            if (statementSpec.getFilterRootNode() != null)
            {
                snapshots[0] = getFiltered(snapshots[0], Arrays.asList(statementSpec.getFilterRootNode()));
            }
            EventBean[] rows = snapshots[0].toArray(new EventBean[snapshots[0].size()]);
            results = resultSetProcessor.processViewResult(rows, null, true);
        }
        else
        {
            Viewable[] viewablePerStream = new Viewable[numStreams];
            for (int i = 0; i < numStreams; i++) {
                FireAndForgetInstance instance = processors[i].getProcessorInstance(agentInstanceContext);
                if (instance == null) {
                    throw new UnsupportedOperationException("Joins against named windows that are under context are not supported");
                }
                viewablePerStream[i] = instance.getTailViewInstance();
            }

            JoinSetComposerDesc joinSetComposerDesc = joinSetComposerPrototype.create(viewablePerStream, true, agentInstanceContext);
            JoinSetComposer joinComposer = joinSetComposerDesc.getJoinSetComposer();
            JoinSetFilter joinFilter;
            if (joinSetComposerDesc.getPostJoinFilterEvaluator() != null) {
                joinFilter = new JoinSetFilter(joinSetComposerDesc.getPostJoinFilterEvaluator());
            }
            else {
                joinFilter = null;
            }

            EventBean[][] oldDataPerStream = new EventBean[numStreams][];
            EventBean[][] newDataPerStream = new EventBean[numStreams][];
            for (int i = 0; i < numStreams; i++)
            {
                newDataPerStream[i] = snapshots[i].toArray(new EventBean[snapshots[i].size()]);
            }
            UniformPair<Set<MultiKey<EventBean>>> result = joinComposer.join(newDataPerStream, oldDataPerStream, agentInstanceContext);
            if (joinFilter != null) {
                joinFilter.process(result.getFirst(), null, agentInstanceContext);
            }
            results = resultSetProcessor.processJoinResult(result.getFirst(), null, true);
        }

        if (statementSpec.getSelectClauseSpec().isDistinct())
        {
            results.setFirst(EventBeanUtility.getDistinctByProp(results.getFirst(), eventBeanReader));
        }

        return new EPPreparedQueryResult(resultSetProcessor.getResultEventType(), results.getFirst());
    }

    private Collection<EventBean> getFiltered(Collection<EventBean> snapshot, List<ExprNode> filterExpressions)
    {
        ArrayDeque<EventBean> deque = new ArrayDeque<EventBean>(Math.min(snapshot.size(), 16));
        ExprNodeUtility.applyFilterExpressionsIterable(snapshot, filterExpressions, agentInstanceContext, deque);
        return deque;
    }

    public EPServicesContext getServices() {
        return services;
    }

    public ExprTableAccessNode[] getTableNodes() {
        return statementSpec.getTableNodes();
    }

    public AgentInstanceContext getAgentInstanceContext() {
        return agentInstanceContext;
    }

    private static class ContextPartitionResult
    {
        private final Collection<EventBean> events;
        private final AgentInstanceContext context;

        private ContextPartitionResult(Collection<EventBean> events, AgentInstanceContext context) {
            this.events = events;
            this.context = context;
        }

        public Collection<EventBean> getEvents() {
            return events;
        }

        public AgentInstanceContext getContext() {
            return context;
        }
    }
}
