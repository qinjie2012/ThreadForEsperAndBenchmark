/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.epl.expression.ops;

import com.espertech.esper.epl.expression.core.ExprEvaluator;
import com.espertech.esper.epl.expression.core.ExprNode;
import com.espertech.esper.type.RelationalOpEnum;

/**
 * Represents a lesser or greater then (</<=/>/>=) expression in a filter expression tree.
 */
public interface ExprRelationalOpNode extends ExprNode, ExprEvaluator
{
    public RelationalOpEnum getRelationalOpEnum();
}
