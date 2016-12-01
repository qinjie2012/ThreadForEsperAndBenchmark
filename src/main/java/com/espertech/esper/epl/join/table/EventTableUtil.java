/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.epl.join.table;

import com.espertech.esper.client.EventType;
import com.espertech.esper.epl.join.plan.QueryPlanIndexItem;
import com.espertech.esper.util.CollectionUtil;

public class EventTableUtil
{
    /**
     * Build an index/table instance using the event properties for the event type.
     *
     * @param indexedStreamNum - number of stream indexed
     * @param eventType - type of event to expect
     * @param optionalIndexName
     * @return table build
     */
    public static EventTable buildIndex(int indexedStreamNum, QueryPlanIndexItem item, EventType eventType, boolean coerceOnAddOnly, boolean unique, String optionalIndexName)
    {
        String[] indexProps = item.getIndexProps();
        Class[] indexCoercionTypes = normalize(item.getOptIndexCoercionTypes());
        String[] rangeProps = item.getRangeProps();
        Class[] rangeCoercionTypes = normalize(item.getOptRangeCoercionTypes());

        EventTable table;
        if (rangeProps == null || rangeProps.length == 0) {
            if (indexProps == null || indexProps.length == 0)
            {
                table = new UnindexedEventTable(indexedStreamNum);
            }
            else
            {
                // single index key
                if (indexProps.length == 1) {
                    if (indexCoercionTypes == null || indexCoercionTypes.length == 0)
                    {
                        PropertyIndexedEventTableSingleFactory factory = new PropertyIndexedEventTableSingleFactory(indexedStreamNum, eventType, indexProps[0], unique, optionalIndexName);
                        table = factory.makeEventTables()[0];
                    }
                    else
                    {
                        if (coerceOnAddOnly) {
                            PropertyIndexedEventTableSingleCoerceAddFactory factory = new PropertyIndexedEventTableSingleCoerceAddFactory(indexedStreamNum, eventType, indexProps[0], indexCoercionTypes[0]);
                            table = factory.makeEventTables()[0];
                        }
                        else {
                            PropertyIndexedEventTableSingleCoerceAllFactory factory = new PropertyIndexedEventTableSingleCoerceAllFactory(indexedStreamNum, eventType, indexProps[0], indexCoercionTypes[0]);
                            table = factory.makeEventTables()[0];
                        }
                    }
                }
                // Multiple index keys
                else {
                    if (indexCoercionTypes == null || indexCoercionTypes.length == 0)
                    {
                        PropertyIndexedEventTableFactory factory = new PropertyIndexedEventTableFactory(indexedStreamNum, eventType, indexProps, unique, optionalIndexName);
                        table = factory.makeEventTables()[0];
                    }
                    else
                    {
                        if (coerceOnAddOnly) {
                            PropertyIndexedEventTableCoerceAddFactory factory = new PropertyIndexedEventTableCoerceAddFactory(indexedStreamNum, eventType, indexProps, indexCoercionTypes);
                            table = factory.makeEventTables()[0];
                        }
                        else {
                            PropertyIndexedEventTableCoerceAllFactory factory = new PropertyIndexedEventTableCoerceAllFactory(indexedStreamNum, eventType, indexProps, indexCoercionTypes);
                            table = factory.makeEventTables()[0];
                        }
                    }
                }
            }
        }
        else {
            if ((rangeProps.length == 1) && (indexProps == null || indexProps.length == 0)) {
                if (rangeCoercionTypes == null) {
                    PropertySortedEventTableFactory factory = new PropertySortedEventTableFactory(indexedStreamNum, eventType, rangeProps[0]);
                    return factory.makeEventTables()[0];
                }
                else {
                    PropertySortedEventTableCoercedFactory factory = new PropertySortedEventTableCoercedFactory(indexedStreamNum, eventType, rangeProps[0], rangeCoercionTypes[0]);
                    return factory.makeEventTables()[0];
                }
            }
            else {
                PropertyCompositeEventTableFactory factory = new PropertyCompositeEventTableFactory(indexedStreamNum, eventType, indexProps, indexCoercionTypes, rangeProps, rangeCoercionTypes);
                return factory.makeEventTables()[0];
            }
        }
        return table;
    }

    private static Class[] normalize(Class[] types) {
        if (types == null) {
            return null;
        }
        if (CollectionUtil.isAllNullArray(types)) {
            return null;
        }
        return types;
    }
}
