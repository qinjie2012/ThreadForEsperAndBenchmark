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
import com.espertech.esper.epl.expression.dot.ExprDotEval;
import com.espertech.esper.epl.expression.core.ExprNode;
import com.espertech.esper.epl.expression.core.ExprValidationContext;
import com.espertech.esper.epl.expression.core.ExprValidationException;

import java.util.List;

public interface ExprDotEvalEnumMethod extends ExprDotEval {

    public void init(Integer streamOfProviderIfApplicable,
                     EnumMethodEnum lambda,
                     String lambdaUsedName,
                     EPType currentInputType,
                     List<ExprNode> parameters,
                     ExprValidationContext validationContext) throws ExprValidationException;
}
