/**
 * Copyright (c) KMG. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.sbk.FoundationDB;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.tuple.Tuple;
import io.sbk.api.DataType;
import io.sbk.api.Parameters;
import io.sbk.api.Reader;
import io.sbk.api.RecordTime;
import io.sbk.api.TimeStamp;

import java.io.EOFException;
import java.io.IOException;

/**
 * Class for Reader.
 */
public class FoundationDBReader implements Reader<byte[]> {
    final private Database db;
    final private Transaction tx;
    private long key;

    public FoundationDBReader(int id, Parameters params, Database db) throws IOException {
        this.key = (id * Integer.MAX_VALUE) + 1;
        this.db = db;
        this.tx = db.createTransaction();
    }

    @Override
    public void recordRead(DataType dType, TimeStamp status, RecordTime recordTime, int id) throws IOException {
        status.startTime = System.currentTimeMillis();
        AsyncIterator<KeyValue> iterator = tx.getRange(Tuple.from(key).pack(),
                Tuple.from(key + 1 + Integer.MAX_VALUE).pack()).
                iterator();
        status.endTime = System.currentTimeMillis();
        status.records = 0;
        if (iterator.hasNext()) {
            status.bytes = 0;
            while (iterator.hasNext()) {
                KeyValue entry = iterator.next();
                status.bytes += entry.getValue().length;
                status.records += 1;
            }
            recordTime.accept(id, status.startTime, status.endTime, status.bytes, status.records);
        }
    }

    @Override
    public void recordReadTime(DataType dType, TimeStamp status, RecordTime recordTime, int id) throws IOException {
        status.startTime = 0;
        AsyncIterator<KeyValue> iterator = tx.getRange(Tuple.from(key).pack(),
                Tuple.from(key + 1 + Integer.MAX_VALUE).pack()).
                iterator();
        status.endTime = System.currentTimeMillis();
        status.records = 0;
        if (iterator.hasNext()) {
            status.bytes = 0;
            while (iterator.hasNext()) {
                KeyValue entry = iterator.next();
                status.bytes += entry.getValue().length;
                status.records += 1;
                if (status.startTime == 0) {
                    status.startTime = dType.getTime(entry.getValue());
                }
            }
            recordTime.accept(id, status.startTime, status.endTime, status.bytes, status.records);
        }
    }


    @Override
    public byte[] read() throws EOFException, IOException {
        byte[] ret;
        ret = db.read(tr -> {
            byte[] result = tr.get(Tuple.from(key).pack()).join();
            return result;
        });
        if (ret != null) {
            key++;
        }
        return ret;
    }

    @Override
    public void close() throws  IOException {
        tx.close();
    }
}