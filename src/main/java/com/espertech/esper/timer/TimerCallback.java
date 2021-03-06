/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.timer;

/**
 * Callback interface for a time provider that triggers at scheduled intervals.
 */
public interface TimerCallback
{
    /**
     * Invoked by the internal clocking service at regular intervals.
     */
    public void timerCallback();
}
