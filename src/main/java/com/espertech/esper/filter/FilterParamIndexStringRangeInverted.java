/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.filter;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.metrics.instrumentation.InstrumentationHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * Index for filter parameter constants for the not range operators (range open/closed/half).
 * The implementation is based on the SortedMap implementation of TreeMap and stores only expression
 * parameter values of type StringRange.
 */
public final class FilterParamIndexStringRangeInverted extends FilterParamIndexStringRangeBase
{
    public FilterParamIndexStringRangeInverted(FilterSpecLookupable lookupable, ReadWriteLock readWriteLock, FilterOperator filterOperator) {
        super(lookupable, readWriteLock, filterOperator);
        if (!(filterOperator.isInvertedRangeOperator()))
        {
            throw new IllegalArgumentException("Invalid filter operator " + filterOperator);
        }
    }

    public final void matchEvent(EventBean theEvent, Collection<FilterHandle> matches)
    {
        Object objAttributeValue = lookupable.getGetter().get(theEvent);
        if (InstrumentationHelper.ENABLED) { InstrumentationHelper.get().qFilterReverseIndex(this, objAttributeValue);}

        if (objAttributeValue == null)
        {
            if (InstrumentationHelper.ENABLED) { InstrumentationHelper.get().aFilterReverseIndex(false);}
            return;
        }

        String attributeValue = (String) objAttributeValue;

        if (this.getFilterOperator() == FilterOperator.NOT_RANGE_CLOSED)   // include all endpoints
        {
            for (Map.Entry<StringRange, EventEvaluator> entry : ranges.entrySet()) {
                if (entry.getKey().getMin().compareTo(attributeValue) > 0 || entry.getKey().getMax().compareTo(attributeValue) < 0) {
                    entry.getValue().matchEvent(theEvent, matches);
                }
            }
        }
        else if (this.getFilterOperator() == FilterOperator.NOT_RANGE_OPEN) {  // include neither endpoint
            for (Map.Entry<StringRange, EventEvaluator> entry : ranges.entrySet()) {
                if (entry.getKey().getMin().compareTo(attributeValue) >= 0 || entry.getKey().getMax().compareTo(attributeValue) <= 0) {
                    entry.getValue().matchEvent(theEvent, matches);
                }
            }
        }
        else if (this.getFilterOperator() == FilterOperator.NOT_RANGE_HALF_CLOSED) // include high endpoint not low endpoint
        {
            for (Map.Entry<StringRange, EventEvaluator> entry : ranges.entrySet()) {
                if (entry.getKey().getMin().compareTo(attributeValue) >= 0 || entry.getKey().getMax().compareTo(attributeValue) < 0) {
                    entry.getValue().matchEvent(theEvent, matches);
                }
            }
        }
        else if (this.getFilterOperator() == FilterOperator.NOT_RANGE_HALF_OPEN) // include low endpoint not high endpoint
        {
            for (Map.Entry<StringRange, EventEvaluator> entry : ranges.entrySet()) {
                if (entry.getKey().getMin().compareTo(attributeValue) > 0 || entry.getKey().getMax().compareTo(attributeValue) <= 0) {
                    entry.getValue().matchEvent(theEvent, matches);
                }
            }
        }
        else
        {
            throw new IllegalStateException("Invalid filter operator " + this.getFilterOperator());
        }
        if (InstrumentationHelper.ENABLED) { InstrumentationHelper.get().aFilterReverseIndex(null);}
    }

    private static final Log log = LogFactory.getLog(FilterParamIndexStringRangeInverted.class);
}
