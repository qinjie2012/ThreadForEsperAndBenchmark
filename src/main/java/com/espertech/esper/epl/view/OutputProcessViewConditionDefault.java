/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.epl.view;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.collection.MultiKey;
import com.espertech.esper.collection.UniformPair;
import com.espertech.esper.core.context.util.AgentInstanceContext;
import com.espertech.esper.epl.core.ResultSetProcessor;
import com.espertech.esper.epl.expression.core.ExprEvaluatorContext;
import com.espertech.esper.event.EventBeanUtility;
import com.espertech.esper.metrics.instrumentation.InstrumentationHelper;
import com.espertech.esper.util.AuditPath;
import com.espertech.esper.util.ExecutionPathDebugLog;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;

/**
 * A view that prepares output events, batching incoming
 * events and invoking the result set processor as necessary.
 * <p>
 * Handles output rate limiting or stabilizing.
 */
public class OutputProcessViewConditionDefault extends OutputProcessViewBaseWAfter
{
    private final OutputProcessViewConditionFactory parent;
    private final OutputCondition outputCondition;

    // Posted events in ordered form (for applying to aggregates) and summarized per type
    // Using ArrayList as random access is a requirement.
    private List<UniformPair<EventBean[]>> viewEventsList = new ArrayList<UniformPair<EventBean[]>>();
	private List<UniformPair<Set<MultiKey<EventBean>>>> joinEventsSet = new ArrayList<UniformPair<Set<MultiKey<EventBean>>>>();

	private static final Log log = LogFactory.getLog(OutputProcessViewConditionDefault.class);

    public OutputProcessViewConditionDefault(ResultSetProcessor resultSetProcessor, Long afterConditionTime, Integer afterConditionNumberOfEvents, boolean afterConditionSatisfied, OutputProcessViewConditionFactory parent, AgentInstanceContext agentInstanceContext) {
        super(resultSetProcessor, afterConditionTime, afterConditionNumberOfEvents, afterConditionSatisfied);
        this.parent = parent;

        OutputCallback outputCallback = getCallbackToLocal(parent.getStreamCount());
        this.outputCondition = parent.getOutputConditionFactory().make(agentInstanceContext, outputCallback);
    }

    public int getNumChangesetRows() {
        return Math.max(viewEventsList.size(), joinEventsSet.size());
    }

    /**
     * The update method is called if the view does not participate in a join.
     * @param newData - new events
     * @param oldData - old events
     */
    public void update(EventBean[] newData, EventBean[] oldData)
    {
        if (InstrumentationHelper.ENABLED) { InstrumentationHelper.get().qOutputProcessWCondition(newData, oldData);}

        if ((ExecutionPathDebugLog.isDebugEnabled) && (log.isDebugEnabled()))
        {
            log.debug(".update Received update, " +
                    "  newData.length==" + ((newData == null) ? 0 : newData.length) +
                    "  oldData.length==" + ((oldData == null) ? 0 : oldData.length));
        }

        // add the incoming events to the event batches
        if (parent.isHasAfter()) {
            boolean afterSatisfied = super.checkAfterCondition(newData, parent.getStatementContext());
            if (!afterSatisfied) {
                if (!parent.isUnaggregatedUngrouped()) {
                    viewEventsList.add(new UniformPair<EventBean[]>(newData, oldData));
                }
                if (InstrumentationHelper.ENABLED) { InstrumentationHelper.get().aOutputProcessWCondition(false);}
                return;
            }
            else {
                viewEventsList.add(new UniformPair<EventBean[]>(newData, oldData));
            }
        }
        else {
            viewEventsList.add(new UniformPair<EventBean[]>(newData, oldData));
        }

        int newDataLength = 0;
        int oldDataLength = 0;
        if(newData != null)
        {
        	newDataLength = newData.length;
        }
        if(oldData != null)
        {
        	oldDataLength = oldData.length;
        }

        if (InstrumentationHelper.ENABLED) { InstrumentationHelper.get().qOutputRateConditionUpdate(newDataLength, oldDataLength);}
        outputCondition.updateOutputCondition(newDataLength, oldDataLength);
        if (InstrumentationHelper.ENABLED) { InstrumentationHelper.get().aOutputRateConditionUpdate();}

        if (InstrumentationHelper.ENABLED) { InstrumentationHelper.get().aOutputProcessWCondition(true);}
    }

    /**
     * This process (update) method is for participation in a join.
     * @param newEvents - new events
     * @param oldEvents - old events
     */
    public void process(Set<MultiKey<EventBean>> newEvents, Set<MultiKey<EventBean>> oldEvents, ExprEvaluatorContext exprEvaluatorContext)
    {
        if (InstrumentationHelper.ENABLED) { InstrumentationHelper.get().qOutputProcessWConditionJoin(newEvents, oldEvents);}

        if ((ExecutionPathDebugLog.isDebugEnabled) && (log.isDebugEnabled()))
        {
            log.debug(".process Received update, " +
                    "  newData.length==" + ((newEvents == null) ? 0 : newEvents.size()) +
                    "  oldData.length==" + ((oldEvents == null) ? 0 : oldEvents.size()));
        }

        // add the incoming events to the event batches
        if (parent.isHasAfter()) {
            boolean afterSatisfied = super.checkAfterCondition(newEvents, parent.getStatementContext());
            if (!afterSatisfied) {
                if (!parent.isUnaggregatedUngrouped()) {
                    addToChangeset(newEvents, oldEvents, joinEventsSet);
                }
                if (InstrumentationHelper.ENABLED) { InstrumentationHelper.get().aOutputProcessWConditionJoin(false);}
                return;
            }
            else {
                addToChangeset(newEvents, oldEvents, joinEventsSet);
            }
        }
        else {
            addToChangeset(newEvents, oldEvents, joinEventsSet);
        }

        int newEventsSize = 0;
        if (newEvents != null)
        {
            newEventsSize = newEvents.size();
        }

        int oldEventsSize = 0;
        if (oldEvents != null)
        {
            oldEventsSize = oldEvents.size();
        }

        if (InstrumentationHelper.ENABLED) { InstrumentationHelper.get().qOutputRateConditionUpdate(newEventsSize, oldEventsSize);}
        outputCondition.updateOutputCondition(newEventsSize, oldEventsSize);
        if (InstrumentationHelper.ENABLED) { InstrumentationHelper.get().aOutputRateConditionUpdate();}

        if (InstrumentationHelper.ENABLED) { InstrumentationHelper.get().aOutputProcessWConditionJoin(true);}
    }

    /**
	 * Called once the output condition has been met.
	 * Invokes the result set processor.
	 * Used for non-join event data.
	 * @param doOutput - true if the batched events should actually be output as well as processed, false if they should just be processed
	 * @param forceUpdate - true if output should be made even when no updating events have arrived
	 * */
	protected void continueOutputProcessingView(boolean doOutput, boolean forceUpdate)
	{
        if (InstrumentationHelper.ENABLED) { InstrumentationHelper.get().qOutputRateConditionOutputNow();}

        if ((ExecutionPathDebugLog.isDebugEnabled) && (log.isDebugEnabled()))
        {
            log.debug(".continueOutputProcessingView");
        }

        boolean isGenerateSynthetic = parent.getStatementResultService().isMakeSynthetic();
        boolean isGenerateNatural = parent.getStatementResultService().isMakeNatural();

        // Process the events and get the result
        UniformPair<EventBean[]> newOldEvents = resultSetProcessor.processOutputLimitedView(viewEventsList, isGenerateSynthetic, parent.getOutputLimitLimitType());

        if (parent.isDistinct() && newOldEvents != null)
        {
            newOldEvents.setFirst(EventBeanUtility.getDistinctByProp(newOldEvents.getFirst(), parent.getEventBeanReader()));
            newOldEvents.setSecond(EventBeanUtility.getDistinctByProp(newOldEvents.getSecond(), parent.getEventBeanReader()));
        }

        if ((!isGenerateSynthetic) && (!isGenerateNatural))
        {
            if (AuditPath.isAuditEnabled) {
                OutputStrategyUtil.indicateEarlyReturn(parent.getStatementContext(), newOldEvents);
            }
            resetEventBatches();
            if (InstrumentationHelper.ENABLED) { InstrumentationHelper.get().aOutputRateConditionOutputNow(false);}
            return;
        }

        if(doOutput)
		{
			output(forceUpdate, newOldEvents);
		}
		resetEventBatches();

        if (InstrumentationHelper.ENABLED) { InstrumentationHelper.get().aOutputRateConditionOutputNow(true);}
    }

	protected void output(boolean forceUpdate, UniformPair<EventBean[]> results)
	{
        // Child view can be null in replay from named window
        if (childView != null)
        {
            OutputStrategyUtil.output(forceUpdate, results, childView);
        }
	}

	private void resetEventBatches()
	{
		viewEventsList.clear();
		joinEventsSet.clear();
    }

	/**
	 * Called once the output condition has been met.
	 * Invokes the result set processor.
	 * Used for join event data.
	 * @param doOutput - true if the batched events should actually be output as well as processed, false if they should just be processed
	 * @param forceUpdate - true if output should be made even when no updating events have arrived
	 */
	protected void continueOutputProcessingJoin(boolean doOutput, boolean forceUpdate)
	{
        if (InstrumentationHelper.ENABLED) { InstrumentationHelper.get().qOutputRateConditionOutputNow();}

        if ((ExecutionPathDebugLog.isDebugEnabled) && (log.isDebugEnabled()))
        {
            log.debug(".continueOutputProcessingJoin");
        }

        boolean isGenerateSynthetic = parent.getStatementResultService().isMakeSynthetic();
        boolean isGenerateNatural = parent.getStatementResultService().isMakeNatural();

        // Process the events and get the result
        UniformPair<EventBean[]> newOldEvents = resultSetProcessor.processOutputLimitedJoin(joinEventsSet, isGenerateSynthetic, parent.getOutputLimitLimitType());

        if (parent.isDistinct() && newOldEvents != null)
        {
            newOldEvents.setFirst(EventBeanUtility.getDistinctByProp(newOldEvents.getFirst(), parent.getEventBeanReader()));
            newOldEvents.setSecond(EventBeanUtility.getDistinctByProp(newOldEvents.getSecond(), parent.getEventBeanReader()));
        }

        if ((!isGenerateSynthetic) && (!isGenerateNatural))
        {
            if (AuditPath.isAuditEnabled) {
                OutputStrategyUtil.indicateEarlyReturn(parent.getStatementContext(), newOldEvents);
            }
            resetEventBatches();
            if (InstrumentationHelper.ENABLED) { InstrumentationHelper.get().aOutputRateConditionOutputNow(false);}
            return;
        }

		if(doOutput)
		{
			output(forceUpdate, newOldEvents);
		}
		resetEventBatches();

        if (InstrumentationHelper.ENABLED) { InstrumentationHelper.get().aOutputRateConditionOutputNow(true);}
    }

    private OutputCallback getCallbackToLocal(int streamCount)
    {
        // single stream means no join
        // multiple streams means a join
        if(streamCount == 1)
        {
            return new OutputCallback()
            {
                public void continueOutputProcessing(boolean doOutput, boolean forceUpdate)
                {
                    OutputProcessViewConditionDefault.this.continueOutputProcessingView(doOutput, forceUpdate);
                }
            };
        }
        else
        {
            return new OutputCallback()
            {
                public void continueOutputProcessing(boolean doOutput, boolean forceUpdate)
                {
                    OutputProcessViewConditionDefault.this.continueOutputProcessingJoin(doOutput, forceUpdate);
                }
            };
        }
    }

    public Iterator<EventBean> iterator() {
        return OutputStrategyUtil.getIterator(joinExecutionStrategy, resultSetProcessor, parentView, parent.isDistinct());
    }

    public void terminated() {
        if (parent.isTerminable()) {
            outputCondition.terminated();
        }
    }

    private static void addToChangeset(Set<MultiKey<EventBean>> newEvents, Set<MultiKey<EventBean>> oldEvents, List<UniformPair<Set<MultiKey<EventBean>>>> joinEventsSet) {
        // add the incoming events to the event batches
        Set<MultiKey<EventBean>> copyNew;
        if (newEvents != null) {
            copyNew = new LinkedHashSet<MultiKey<EventBean>>(newEvents);
        }
        else {
            copyNew = new LinkedHashSet<MultiKey<EventBean>>();
        }

        Set<MultiKey<EventBean>> copyOld;
        if (oldEvents != null) {
            copyOld = new LinkedHashSet<MultiKey<EventBean>>(oldEvents);
        }
        else {
            copyOld = new LinkedHashSet<MultiKey<EventBean>>();
        }

        joinEventsSet.add(new UniformPair<Set<MultiKey<EventBean>>>(copyNew, copyOld));
    }
}
