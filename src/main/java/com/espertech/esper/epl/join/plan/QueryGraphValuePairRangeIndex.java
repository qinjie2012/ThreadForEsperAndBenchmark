/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.epl.join.plan;

import java.util.List;

public class QueryGraphValuePairRangeIndex
{
    private final String[] indexed;
    private final List<QueryGraphValueEntryRange> key;

    public QueryGraphValuePairRangeIndex(String[] indexed, List<QueryGraphValueEntryRange> key) {
        this.indexed = indexed;
        this.key = key;
    }

    public String[] getIndexed() {
        return indexed;
    }

    public List<QueryGraphValueEntryRange> getKeys() {
        return key;
    }
}

