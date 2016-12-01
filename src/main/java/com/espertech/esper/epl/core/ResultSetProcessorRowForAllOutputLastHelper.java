/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.epl.core;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.collection.MultiKey;
import com.espertech.esper.collection.UniformPair;

import java.util.Set;

public class ResultSetProcessorRowForAllOutputLastHelper
{
    private final ResultSetProcessorRowForAll processor;
    private EventBean[] lastEventRStreamForOutputLast;

    public ResultSetProcessorRowForAllOutputLastHelper(ResultSetProcessorRowForAll processor) {
        this.processor = processor;
    }

    public void processView(EventBean[] newData, EventBean[] oldData, boolean isGenerateSynthetic) {
        if (processor.prototype.isSelectRStream() && lastEventRStreamForOutputLast == null) {
            lastEventRStreamForOutputLast = processor.getSelectListEvents(false, isGenerateSynthetic, false);
        }

        EventBean[] eventsPerStream = new EventBean[1];
        ResultSetProcessorUtil.applyAggViewResult(processor.aggregationService, processor.exprEvaluatorContext, newData, oldData, eventsPerStream);
    }

    public void processJoin(Set<MultiKey<EventBean>> newEvents, Set<MultiKey<EventBean>> oldEvents, boolean isGenerateSynthetic) {
        if (processor.prototype.isSelectRStream() && lastEventRStreamForOutputLast == null) {
            lastEventRStreamForOutputLast = processor.getSelectListEvents(false, isGenerateSynthetic, true);
        }

        ResultSetProcessorUtil.applyAggJoinResult(processor.aggregationService, processor.exprEvaluatorContext, newEvents, oldEvents);
    }

    public UniformPair<EventBean[]> outputView(boolean isSynthesize) {
        return continueOutputLimitedLastNonBuffered(isSynthesize);
    }

    public UniformPair<EventBean[]> outputJoin(boolean isSynthesize) {
        return continueOutputLimitedLastNonBuffered(isSynthesize);
    }

    private UniformPair<EventBean[]> continueOutputLimitedLastNonBuffered(boolean isSynthesize) {
        EventBean[] events = processor.getSelectListEvents(true, isSynthesize, false);
        UniformPair<EventBean[]> result = new UniformPair<EventBean[]>(events, null);

        if (processor.prototype.isSelectRStream() && lastEventRStreamForOutputLast == null) {
            lastEventRStreamForOutputLast = processor.getSelectListEvents(false, isSynthesize, false);
        }
        if (lastEventRStreamForOutputLast != null) {
            result.setSecond(lastEventRStreamForOutputLast);
            lastEventRStreamForOutputLast = null;
        }

        return result;
    }

}
