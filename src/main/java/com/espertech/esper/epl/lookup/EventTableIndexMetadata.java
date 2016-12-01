/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.epl.lookup;

import com.espertech.esper.collection.Pair;
import com.espertech.esper.epl.expression.core.ExprValidationException;

import java.util.*;

public class EventTableIndexMetadata
{
    private final Map<IndexMultiKey, EventTableIndexMetadataEntry> indexes = new HashMap<IndexMultiKey, EventTableIndexMetadataEntry>();

    public EventTableIndexMetadata() {
    }

    public void addIndex(boolean isPrimary, IndexMultiKey indexMultiKey, String explicitIndexName, String statementName, boolean failIfExists)
        throws ExprValidationException
    {
        if (getIndexByName(explicitIndexName) != null) {
            throw new ExprValidationException("An index by name '" + explicitIndexName + "' already exists");
        }
        if (indexes.containsKey(indexMultiKey)) {
            if (failIfExists) {
                throw new ExprValidationException("An index for the same columns already exists");
            }
            return;
        }
        EventTableIndexMetadataEntry entry = new EventTableIndexMetadataEntry(explicitIndexName, isPrimary);
        entry.addReferringStatement(statementName);
        indexes.put(indexMultiKey, entry);
    }

    public Map<IndexMultiKey, EventTableIndexMetadataEntry> getIndexes() {
        return indexes;
    }

    public void removeIndex(IndexMultiKey imk) {
        indexes.remove(imk);
    }

    public boolean removeIndexReference(IndexMultiKey index, String referringStatementName) {
        EventTableIndexMetadataEntry entry = indexes.get(index);
        if (entry == null) {
            return false;
        }
        return entry.removeReferringStatement(referringStatementName);
    }

    public void addIndexReference(String indexName, String statementName) {
        Map.Entry<IndexMultiKey, EventTableIndexMetadataEntry> entry = findIndex(indexName);
        if (entry == null) {
            return;
        }
        entry.getValue().addReferringStatement(statementName);
    }

    public void addIndexReference(IndexMultiKey indexMultiKey, String statementName) {
        EventTableIndexMetadataEntry entry = indexes.get(indexMultiKey);
        if (entry == null) {
            return;
        }
        entry.addReferringStatement(statementName);
    }

    public IndexMultiKey getIndexByName(String indexName) {
        Map.Entry<IndexMultiKey, EventTableIndexMetadataEntry> entry = findIndex(indexName);
        if (entry == null) {
            return null;
        }
        return entry.getKey();
    }

    public Collection<String> getRemoveRefIndexesDereferenced(String statementName) {
        Collection<String> indexNamesDerrefd = null;
        for (Map.Entry<IndexMultiKey, EventTableIndexMetadataEntry> entry : indexes.entrySet()) {
            boolean last = entry.getValue().removeReferringStatement(statementName);
            if (last) {
                if (indexNamesDerrefd == null) {
                    indexNamesDerrefd = new ArrayDeque<String>(2);
                }
                indexNamesDerrefd.add(entry.getValue().getOptionalIndexName());
            }
        }
        if (indexNamesDerrefd == null) {
            return Collections.emptyList();
        }
        for (String name : indexNamesDerrefd) {
            removeIndex(getIndexByName(name));
        }
        return indexNamesDerrefd;
    }

    private Map.Entry<IndexMultiKey, EventTableIndexMetadataEntry> findIndex(String indexName) {
        for (Map.Entry<IndexMultiKey, EventTableIndexMetadataEntry> entry : indexes.entrySet()) {
            if (entry.getValue().getOptionalIndexName() != null && entry.getValue().getOptionalIndexName().equals(indexName)) {
                return entry;
            }
        }
        return null;
    }

    public String[][] getUniqueIndexProps() {
        ArrayDeque<String[]> uniques = new ArrayDeque<String[]>(2);
        for (Map.Entry<IndexMultiKey, EventTableIndexMetadataEntry> entry : indexes.entrySet()) {
            if (entry.getKey().isUnique()) {
                String[] props = new String[entry.getKey().getHashIndexedProps().length];
                for (int i = 0; i < entry.getKey().getHashIndexedProps().length; i++) {
                    props[i] = entry.getKey().getHashIndexedProps()[i].getIndexPropName();
                }
                uniques.add(props);
            }
        }
        return uniques.toArray(new String[uniques.size()][]);
    }
}
