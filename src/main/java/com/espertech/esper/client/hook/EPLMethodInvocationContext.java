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

package com.espertech.esper.client.hook;

/**
 * Invocation context for method invocations that invoke static methods or plug-in single-row functions.
 */
public class EPLMethodInvocationContext {
    private final String statementName;
    private final int contextPartitionId;
    private final String engineURI;
    private final String functionName;
    private final Object statementUserObject;

    /**
     * Ctor.
     * @param statementName the statement name
     * @param contextPartitionId context partition id if using contexts, or -1 if not using context partitions
     * @param engineURI the engine URI
     * @param functionName the name of the plug-in single row function, or the method name if not a plug-in single row function
     * @param statementUserObject the statement user object or null if not assigned
     */
    public EPLMethodInvocationContext(String statementName, int contextPartitionId, String engineURI, String functionName, Object statementUserObject) {
        this.statementName = statementName;
        this.contextPartitionId = contextPartitionId;
        this.engineURI = engineURI;
        this.functionName = functionName;
        this.statementUserObject = statementUserObject;
    }

    /**
     * Returns the statement name.
     * @return statement name
     */
    public String getStatementName() {
        return statementName;
    }

    /**
     * Returns the context partition id, or -1 if no contexts
     * @return context partition id
     */
    public int getContextPartitionId() {
        return contextPartitionId;
    }

    /**
     * Returns the engine URI
     * @return engine URI
     */
    public String getEngineURI() {
        return engineURI;
    }

    /**
     * Returns the function name that appears in the EPL statement.
     * @return function name
     */
    public String getFunctionName() {
        return functionName;
    }

    /**
     * Returns the statement user object or null if not assigned
     * @return statement user object
     */
    public Object getStatementUserObject() {
        return statementUserObject;
    }
}
