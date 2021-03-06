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

package com.espertech.esper.epl.enummethod.dot;

import com.espertech.esper.epl.rettype.EPType;
import com.espertech.esper.epl.rettype.EPTypeHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;

public class ExprDotStaticMethodWrapArrayScalar implements ExprDotStaticMethodWrap {
    private static final Log log = LogFactory.getLog(ExprDotStaticMethodWrapArrayScalar.class);

    private final String methodName;
    private final Class componentType;

    public ExprDotStaticMethodWrapArrayScalar(String methodName, Class componentType) {
        this.methodName = methodName;
        this.componentType = componentType;
    }

    public EPType getTypeInfo() {
        return EPTypeHelper.collectionOfSingleValue(componentType);
    }

    public Collection convert(Object result) {
        if (result == null) {
            return null;
        }
        if (!result.getClass().isArray()) {
            log.warn("Expected array-type input from method '" + methodName + "' but received " + result.getClass());
            return null;
        }
        return new ArrayWrappingCollection(result);
    }
}
