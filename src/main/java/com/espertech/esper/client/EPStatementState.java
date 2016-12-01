/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.client;

/**
 * Enumerates all statement states.
 */
public enum EPStatementState
{
    /**
     * Started state.
     */
    STARTED,

    /**
     * Stopped state.
     */
    STOPPED,

    /**
     * Destroyed state.
     */
    DESTROYED,

    /**
     * Failed state, equivalent to STOPPED state and reserved for a failed recovery of statement state; Not applicable to core engine.
     */
    FAILED
}
