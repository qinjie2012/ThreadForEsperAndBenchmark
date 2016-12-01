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

package com.espertech.esper.rowregex;

public class MatchRecognizeStatePoolStmtHandler {

    private int count;

    public int getCount() {
        return count;
    }

    public void decreaseCount() {
        count--;
        if (count < 0) {
            count = 0;
        }
    }

    public void decreaseCount(int num) {
        count-=num;
        if (count < 0) {
            count = 0;
        }
    }

    public void increaseCount() {
        count++;
    }
}
