/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.client.soda;

import java.io.StringWriter;

/**
 * Maximum of the (distinct) values returned by an expression.
 */
public class MaxProjectionExpression extends ExpressionBase
{
    private boolean distinct;
    private static final long serialVersionUID = -2052925266576308551L;
    private boolean ever;

    /**
     * Ctor.
     */
    public MaxProjectionExpression() {
    }

    /**
     * Ctor - for use to create an expression tree, without inner expression
     * @param isDistinct true if distinct
     */
    public MaxProjectionExpression(boolean isDistinct)
    {
        this.distinct = isDistinct;
    }

    /**
     * Ctor - for use to create an expression tree, without inner expression
     * @param isDistinct true if distinct
     * @param isEver indicator max-ever
     */
    public MaxProjectionExpression(boolean isDistinct, boolean isEver)
    {
        this.distinct = isDistinct;
        this.ever = isEver;
    }

    /**
     * Ctor - adds the expression to project.
     * @param expression returning values to project
     * @param isDistinct true if distinct
     */
    public MaxProjectionExpression(Expression expression, boolean isDistinct)
    {
        this.distinct = isDistinct;
        this.getChildren().add(expression);
    }

    public ExpressionPrecedenceEnum getPrecedence()
    {
        return ExpressionPrecedenceEnum.UNARY;
    }

    public void toPrecedenceFreeEPL(StringWriter writer)
    {
        String name;
        if (this.getChildren().size() > 1) {
            name = "fmax";
        }
        else {
            if (ever) {
                name = "maxever";
            }
            else {
                name = "max";
            }
        }
        ExpressionBase.renderAggregation(writer, name, distinct, this.getChildren());
    }

    /**
     * Returns true if the projection considers distinct values only.
     * @return true if distinct
     */
    public boolean isDistinct()
    {
        return distinct;
    }

    /**
     * Returns true if the projection considers distinct values only.
     * @return true if distinct
     */
    public boolean getDistinct()
    {
        return distinct;
    }

    /**
     * Set the distinct flag indicating the projection considers distinct values only.
     * @param distinct true for distinct, false for not distinct
     */
    public void setDistinct(boolean distinct)
    {
        this.distinct = distinct;
    }

    /**
     * Returns true for max-ever
     * @return indicator for "ever"
     */
    public boolean isEver() {
        return ever;
    }

    /**
     * Set to true for max-ever
     * @param ever indicator for "ever"
     */
    public void setEver(boolean ever) {
        this.ever = ever;
    }
}
