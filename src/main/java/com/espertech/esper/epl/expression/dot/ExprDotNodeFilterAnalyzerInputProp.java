/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.epl.expression.dot;

public class ExprDotNodeFilterAnalyzerInputProp implements ExprDotNodeFilterAnalyzerInput
{
    private final int streamNum;
    private final String propertyName;

    public ExprDotNodeFilterAnalyzerInputProp(int streamNum, String propertyName) {
        this.streamNum = streamNum;
        this.propertyName = propertyName;
    }

    public int getStreamNum() {
        return streamNum;
    }

    public String getPropertyName() {
        return propertyName;
    }
}
