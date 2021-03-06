/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.core.service;

import com.espertech.esper.epl.expression.subquery.ExprSubselectNode;
import com.espertech.esper.epl.spec.StatementSpecRaw;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Map;

/**
 * Interface for a factory class that makes statement context specific to a statement.
 */
public interface StatementContextFactory
{
    public void setStmtEngineServices(EPServicesContext services);

    /**
     * Create a new statement context consisting of statement-level services.
     *
     * @param statementId is the statement is
     * @param statementName is the statement name
     * @param expression is the statement expression
     * @param engineServices is engine services
     * @param optAdditionalContext addtional context to pass to the statement
     * @param isFireAndForget if the statement context is for a fire-and-forget statement
     * @param annotations statement annotations
     * @param isolationUnitServices for isolation units
     * @param subselectNodes
     * @return statement context
     */
    public StatementContext makeContext(String statementId,
                                        String statementName,
                                        String expression,
                                        StatementType statementType,
                                        EPServicesContext engineServices,
                                        Map<String, Object> optAdditionalContext,
                                        boolean isFireAndForget,
                                        Annotation[] annotations,
                                        EPIsolationUnitServices isolationUnitServices,
                                        boolean stateless,
                                        StatementSpecRaw statementSpecRaw,
                                        List<ExprSubselectNode> subselectNodes,
                                        boolean writesToTables,
                                        Object statementUserObject);
}
