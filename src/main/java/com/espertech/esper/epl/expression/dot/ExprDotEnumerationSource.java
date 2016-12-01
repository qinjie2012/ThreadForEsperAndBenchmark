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

package com.espertech.esper.epl.expression.dot;

import com.espertech.esper.epl.expression.core.ExprEvaluatorEnumeration;
import com.espertech.esper.epl.rettype.EPType;

public class ExprDotEnumerationSource {
    private final EPType returnType;
    private final Integer streamOfProviderIfApplicable;
    private final ExprEvaluatorEnumeration enumeration;

    public ExprDotEnumerationSource(EPType returnType, Integer streamOfProviderIfApplicable, ExprEvaluatorEnumeration enumeration) {
        this.returnType = returnType;
        this.streamOfProviderIfApplicable = streamOfProviderIfApplicable;
        this.enumeration = enumeration;
    }

    public ExprEvaluatorEnumeration getEnumeration() {
        return enumeration;
    }

    public EPType getReturnType() {
        return returnType;
    }

    public Integer getStreamOfProviderIfApplicable() {
        return streamOfProviderIfApplicable;
    }
}
