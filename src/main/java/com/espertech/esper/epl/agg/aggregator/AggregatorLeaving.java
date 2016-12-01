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
 * For testing if a remove stream entry has been present.
 */
public class AggregatorLeaving implements AggregationMethod {

    protected boolean leaving = false;

    public void enter(Object value) {
    }

    public void leave(Object value) {
        leaving = true;
    }

    public Class getValueType() {
        return Boolean.class;
    }

    public Object getValue() {
        return leaving;
    }

    public void clear() {
        leaving = false;
    }
}