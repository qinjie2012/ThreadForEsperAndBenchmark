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

package com.espertech.esper.epl.datetime.interval;

import com.espertech.esper.epl.core.StreamTypeService;
import com.espertech.esper.epl.datetime.eval.DatetimeMethodEnum;
import com.espertech.esper.epl.datetime.eval.OpFactory;
import com.espertech.esper.epl.expression.core.ExprEvaluator;
import com.espertech.esper.epl.expression.core.ExprNode;
import com.espertech.esper.epl.expression.core.ExprValidationException;

import java.util.List;

public class IntervalOpFactory implements OpFactory {
    public IntervalOp getOp(StreamTypeService streamTypeService, DatetimeMethodEnum method, String methodNameUsed, List<ExprNode> parameters, ExprEvaluator[] evaluators)
        throws ExprValidationException {

        return new IntervalOpImpl(method, methodNameUsed, streamTypeService, parameters);
    }

}
