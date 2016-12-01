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
import com.espertech.esper.epl.agg.service.AggregationGroupByRollupLevel;

import java.util.*;

public class ResultSetProcessorRowPerGroupRollupOutputLastHelper {

    private final ResultSetProcessorRowPerGroupRollup processor;
    private final Map<Object, EventBean>[] groupRepsOutputLastUnordRStream;

    public ResultSetProcessorRowPerGroupRollupOutputLastHelper(ResultSetProcessorRowPerGroupRollup processor, int levelCount) {
        this.processor = processor;

        groupRepsOutputLastUnordRStream = (LinkedHashMap<Object, EventBean>[]) new LinkedHashMap[levelCount];
        for (int i = 0; i < levelCount; i++) {
            groupRepsOutputLastUnordRStream[i] = new LinkedHashMap<Object, EventBean>();
        }
    }

    public void processView(EventBean[] newData, EventBean[] oldData, boolean isGenerateSynthetic) {
        // apply to aggregates
        Object[] groupKeysPerLevel = new Object[processor.prototype.getGroupByRollupDesc().getLevels().length];
        EventBean[] eventsPerStream;
        if (newData != null) {
            for (EventBean aNewData : newData) {
                eventsPerStream = new EventBean[] {aNewData};
                Object groupKeyComplete = processor.generateGroupKey(eventsPerStream, true);
                for (AggregationGroupByRollupLevel level : processor.prototype.getGroupByRollupDesc().getLevels()) {
                    Object groupKey = level.computeSubkey(groupKeyComplete);
                    groupKeysPerLevel[level.getLevelNumber()] = groupKey;

                    processor.outputLimitGroupRepsPerLevel[level.getLevelNumber()].put(groupKey, eventsPerStream);
                    if (processor.prototype.isSelectRStream() && !groupRepsOutputLastUnordRStream[level.getLevelNumber()].containsKey(groupKey)) {
                        processor.generateOutputBatchedMapUnsorted(false, groupKey, level, eventsPerStream, true, isGenerateSynthetic, groupRepsOutputLastUnordRStream[level.getLevelNumber()]);
                    }
                }
                processor.aggregationService.applyEnter(eventsPerStream, groupKeysPerLevel, processor.agentInstanceContext);
            }
        }
        if (oldData != null) {
            for (EventBean anOldData : oldData) {
                eventsPerStream = new EventBean[] {anOldData};
                Object groupKeyComplete = processor.generateGroupKey(eventsPerStream, false);
                for (AggregationGroupByRollupLevel level : processor.prototype.getGroupByRollupDesc().getLevels()) {
                    Object groupKey = level.computeSubkey(groupKeyComplete);
                    groupKeysPerLevel[level.getLevelNumber()] = groupKey;

                    processor.outputLimitGroupRepsPerLevel[level.getLevelNumber()].put(groupKey, eventsPerStream);
                    if (processor.prototype.isSelectRStream() && !groupRepsOutputLastUnordRStream[level.getLevelNumber()].containsKey(groupKey)) {
                        processor.generateOutputBatchedMapUnsorted(false, groupKey, level, eventsPerStream, false, isGenerateSynthetic, groupRepsOutputLastUnordRStream[level.getLevelNumber()]);
                    }
                }
                processor.aggregationService.applyLeave(eventsPerStream, groupKeysPerLevel, processor.agentInstanceContext);
            }
        }
    }

    public void processJoin(Set<MultiKey<EventBean>> newEvents, Set<MultiKey<EventBean>> oldEvents, boolean isGenerateSynthetic) {
        // apply to aggregates
        Object[] groupKeysPerLevel = new Object[processor.prototype.getGroupByRollupDesc().getLevels().length];
        if (newEvents != null) {
            for (MultiKey<EventBean> newEvent : newEvents) {
                EventBean[] aNewData = newEvent.getArray();
                Object groupKeyComplete = processor.generateGroupKey(aNewData, true);
                for (AggregationGroupByRollupLevel level : processor.prototype.getGroupByRollupDesc().getLevels()) {
                    Object groupKey = level.computeSubkey(groupKeyComplete);
                    groupKeysPerLevel[level.getLevelNumber()] = groupKey;

                    processor.outputLimitGroupRepsPerLevel[level.getLevelNumber()].put(groupKey, aNewData);
                    if (processor.prototype.isSelectRStream() && !groupRepsOutputLastUnordRStream[level.getLevelNumber()].containsKey(groupKey)) {
                        processor.generateOutputBatchedMapUnsorted(false, groupKey, level, aNewData, true, isGenerateSynthetic, groupRepsOutputLastUnordRStream[level.getLevelNumber()]);
                    }
                }
                processor.aggregationService.applyEnter(aNewData, groupKeysPerLevel, processor.agentInstanceContext);
            }
        }
        if (oldEvents != null) {
            for (MultiKey<EventBean> oldEvent : oldEvents) {
                EventBean[] aOldData = oldEvent.getArray();
                Object groupKeyComplete = processor.generateGroupKey(aOldData, false);
                for (AggregationGroupByRollupLevel level : processor.prototype.getGroupByRollupDesc().getLevels()) {
                    Object groupKey = level.computeSubkey(groupKeyComplete);
                    groupKeysPerLevel[level.getLevelNumber()] = groupKey;

                    processor.outputLimitGroupRepsPerLevel[level.getLevelNumber()].put(groupKey, aOldData);
                    if (processor.prototype.isSelectRStream() && !groupRepsOutputLastUnordRStream[level.getLevelNumber()].containsKey(groupKey)) {
                        processor.generateOutputBatchedMapUnsorted(false, groupKey, level, aOldData, false, isGenerateSynthetic, groupRepsOutputLastUnordRStream[level.getLevelNumber()]);
                    }
                }
                processor.aggregationService.applyLeave(aOldData, groupKeysPerLevel, processor.agentInstanceContext);
            }
        }
    }

    public UniformPair<EventBean[]> outputView(boolean isSynthesize) {
        return output(isSynthesize, false);
    }

    public UniformPair<EventBean[]> outputJoin(boolean isSynthesize) {
        return output(isSynthesize, true);
    }

    private UniformPair<EventBean[]> output(boolean isSynthesize, boolean isJoin) {

        List<EventBean> newEvents = new ArrayList<EventBean>(4);
        for (AggregationGroupByRollupLevel level : processor.prototype.getGroupByRollupDesc().getLevels()) {
            Map<Object, EventBean[]> groupGenerators = processor.outputLimitGroupRepsPerLevel[level.getLevelNumber()];
            for (Map.Entry<Object, EventBean[]> entry : groupGenerators.entrySet()) {
                processor.generateOutputBatched(isJoin, entry.getKey(), level, entry.getValue(), true, isSynthesize, newEvents, null);
            }
        }
        EventBean[] newEventsArr = (newEvents.isEmpty()) ? null : newEvents.toArray(new EventBean[newEvents.size()]);
        for (Map<Object, EventBean[]> outputLimitGroupRepsPerLevelItem : processor.outputLimitGroupRepsPerLevel) {
            outputLimitGroupRepsPerLevelItem.clear();
        }

        EventBean[] oldEventsArr = null;
        if (groupRepsOutputLastUnordRStream != null) {
            List<EventBean> oldEventList = new ArrayList<EventBean>(4);
            for (Map<Object, EventBean> entry : groupRepsOutputLastUnordRStream) {
                oldEventList.addAll(entry.values());
            }
            if (!oldEventList.isEmpty()) {
                oldEventsArr = oldEventList.toArray(new EventBean[oldEventList.size()]);
                for (Map<Object, EventBean> groupRepsOutputLastUnordRStreamItem : groupRepsOutputLastUnordRStream) {
                    groupRepsOutputLastUnordRStreamItem.clear();
                }
            }
        }

        if (newEventsArr == null && oldEventsArr == null) {
            return null;
        }
        return new UniformPair<EventBean[]>(newEventsArr, oldEventsArr);
    }
}
