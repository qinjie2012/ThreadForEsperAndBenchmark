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

package com.espertech.esper.client.dataflow.io;

import com.espertech.esper.event.EventBeanUtility;
import com.espertech.esper.util.SerializerUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

/**
 * Reads a {@link java.io.Serializable} from {@link java.io.DataInput} and emits the resulting object.
 * <p>
 *     The input must carry an int-typed number of bytes followed by the serialized object.
 * </p>
 */
public class DataInputToObjectCollectorSerializable implements DataInputToObjectCollector {
    private static final Log log = LogFactory.getLog(DataInputToObjectCollectorSerializable.class);

    public void collect(DataInputToObjectCollectorContext context) throws IOException {
        int size = context.getDataInput().readInt();
        byte[] bytes = new byte[size];
        context.getDataInput().readFully(bytes);
        Object event = SerializerUtil.byteArrToObject(bytes);
        if (log.isDebugEnabled()) {
            log.debug("Submitting event " + EventBeanUtility.summarizeUnderlying(event));
        }
        context.getEmitter().submit(event);
    }
}
