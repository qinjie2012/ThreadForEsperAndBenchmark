/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.epl.agg.aggregator;

/**
 * Aggregator for count-ever value.
 */
public class AggregatorCountEver implements AggregationMethod
{
    protected long count;

    /**
     * Ctor.
     */
    public AggregatorCountEver() {
    }

    public void clear()
    {
        count = 0;
    }

    public void enter(Object object)
    {
        count++;
    }

    public void leave(Object object)
    {
    }

    public Object getValue()
    {
        return count;
    }

    public Class getValueType()
    {
        return long.class;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
}