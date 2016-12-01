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

package com.espertech.esper.epl.table.mgmt;

import com.espertech.esper.collection.MultiKeyUntyped;
import com.espertech.esper.epl.agg.access.AggregationState;
import com.espertech.esper.epl.agg.aggregator.AggregationMethod;
import com.espertech.esper.epl.agg.service.AggregationMethodFactory;
import com.espertech.esper.epl.agg.service.AggregationRowPair;
import com.espertech.esper.epl.agg.service.AggregationStateFactory;
import com.espertech.esper.epl.core.MethodResolutionService;
import com.espertech.esper.event.EventAdapterService;
import com.espertech.esper.event.ObjectArrayBackedEventBean;
import com.espertech.esper.event.arr.ObjectArrayEventType;

public class TableStateRowFactory {

    private final ObjectArrayEventType objectArrayEventType;
    private final MethodResolutionService methodResolutionService;
    private final AggregationMethodFactory[] methodFactories;
    private final AggregationStateFactory[] stateFactories;
    private final int[] groupKeyIndexes;
    private final EventAdapterService eventAdapterService;

    public TableStateRowFactory(ObjectArrayEventType objectArrayEventType, MethodResolutionService methodResolutionService, AggregationMethodFactory[] methodFactories, AggregationStateFactory[] stateFactories, int[] groupKeyIndexes, EventAdapterService eventAdapterService) {
        this.objectArrayEventType = objectArrayEventType;
        this.methodResolutionService = methodResolutionService;
        this.methodFactories = methodFactories;
        this.stateFactories = stateFactories;
        this.groupKeyIndexes = groupKeyIndexes;
        this.eventAdapterService = eventAdapterService;
    }

    public ObjectArrayBackedEventBean makeOA(int agentInstanceId, Object groupByKey, Object groupKeyBinding) {
        AggregationRowPair row = makeAggs(agentInstanceId, groupByKey, groupKeyBinding);
        Object[] data = new Object[objectArrayEventType.getPropertyDescriptors().length];
        data[0] = row;

        if (groupKeyIndexes.length == 1) {
            data[groupKeyIndexes[0]] = groupByKey;
        }
        else {
            if (groupKeyIndexes.length > 1) {
                Object[] keys = ((MultiKeyUntyped) groupByKey).getKeys();
                for (int i = 0; i < groupKeyIndexes.length; i++) {
                    data[groupKeyIndexes[i]] = keys[i];
                }
            }
        }

        return (ObjectArrayBackedEventBean) eventAdapterService.adapterForType(data, objectArrayEventType);
    }

    public AggregationRowPair makeAggs(int agentInstanceId, Object groupByKey, Object groupKeyBinding) {
        AggregationMethod[] methods = methodResolutionService.newAggregators(methodFactories, agentInstanceId, groupByKey, groupKeyBinding, null);
        AggregationState[] states = methodResolutionService.newAccesses(agentInstanceId, false, stateFactories, groupByKey, groupKeyBinding, null);   return new AggregationRowPair(methods, states);
    }
}
