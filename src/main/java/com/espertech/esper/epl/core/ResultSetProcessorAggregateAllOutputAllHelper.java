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
import com.espertech.esper.event.EventBeanUtility;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Set;

public class ResultSetProcessorAggregateAllOutputAllHelper
{
    private final ResultSetProcessorAggregateAll processor;
    private final Deque<EventBean> eventsOld = new ArrayDeque<EventBean>(2);
    private final Deque<EventBean> eventsNew = new ArrayDeque<EventBean>(2);

    public ResultSetProcessorAggregateAllOutputAllHelper(ResultSetProcessorAggregateAll processor) {
        this.processor = processor;
    }

    public void processView(EventBean[] newData, EventBean[] oldData, boolean isGenerateSynthetic) {
        UniformPair<EventBean[]> pair = processor.processViewResult(newData, oldData, isGenerateSynthetic);
        apply(pair);
    }

    public void processJoin(Set<MultiKey<EventBean>> newEvents, Set<MultiKey<EventBean>> oldEvents, boolean isGenerateSynthetic) {
        UniformPair<EventBean[]> pair = processor.processJoinResult(newEvents, oldEvents, isGenerateSynthetic);
        apply(pair);
    }

    public UniformPair<EventBean[]> output() {
        EventBean[] oldEvents = EventBeanUtility.toArrayNullIfEmpty(eventsOld);
        EventBean[] newEvents = EventBeanUtility.toArrayNullIfEmpty(eventsNew);

        UniformPair<EventBean[]> result = null;
        if (oldEvents != null || newEvents != null) {
            result = new UniformPair<EventBean[]>(newEvents, oldEvents);
        }

        eventsOld.clear();
        eventsNew.clear();
        return result;
    }

    private void apply(UniformPair<EventBean[]> pair) {
        if (pair == null) {
            return;
        }
        EventBeanUtility.addToCollection(pair.getFirst(), eventsNew);
        EventBeanUtility.addToCollection(pair.getSecond(), eventsOld);
    }
}
