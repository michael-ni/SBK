/**
 * Copyright (c) KMG. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.sbk.MongoDB;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import io.sbk.api.DataType;
import io.sbk.api.Parameters;
import io.sbk.api.Reader;
import io.sbk.api.RecordTime;
import io.sbk.api.Status;
import org.bson.Document;
import org.bson.types.Binary;

import java.io.EOFException;
import java.io.IOException;

/**
 * Class for Reader.
 */
public class MongoDBMultiReader implements Reader<byte[]> {
    final private  MongoCollection<Document> databaseCollection;
    final private Parameters params;
    private long key;
    private int cnt;
    private MongoCursor<Document> cursor;

    public MongoDBMultiReader(int id, Parameters params, MongoDBConfig config,  MongoCollection<Document> databaseCollection) throws IOException {
        this.key = MongoDB.generateStartKey(id);
        this.cnt = 0;
        this.params = params;
        this.databaseCollection = databaseCollection;
    }

    @Override
    public byte[] read() throws EOFException, IOException {
        if (cursor == null) {
             cursor = databaseCollection.find().iterator();
        }
        if (cursor.hasNext()) {
            key++;
            Binary bin = cursor.next().get("data", org.bson.types.Binary.class);
            return bin.getData();
        }
        return null;
    }

    @Override
    public void close() throws  IOException {
    }

    @Override
    public void recordRead(DataType<byte[]> dType, Status status, RecordTime recordTime, int id)
            throws EOFException, IOException {
        final int recs;
        byte[] result;
        if (params.getRecordsPerReader() > 0 && params.getRecordsPerReader() > cnt) {
            recs = Math.min(params.getRecordsPerReader() - cnt, params.getRecordsPerSync());
        } else {
            recs =  params.getRecordsPerSync();
        }
        status.startTime = System.currentTimeMillis();
        if (cursor == null) {
            cursor = databaseCollection.find().iterator();
        }
        int i = 0;
        status.bytes = 0;
        status.records = 0;
        while (cursor.hasNext() && i < recs) {
            key++;
            Binary bin = cursor.next().get("data", org.bson.types.Binary.class);
            result = bin.getData();
            if (result != null) {
                status.bytes += result.length;
                status.records += 1;
            }
            i++;
        }

        if (status.records == 0) {
            throw new EOFException();
        }
        status.endTime = System.currentTimeMillis();
        key += recs;
        cnt += recs;
        recordTime.accept(id, status.startTime, status.endTime, status.bytes, status.records);
    }


    @Override
    public void recordReadTime(DataType<byte[]> dType, Status status, RecordTime recordTime, int id)
            throws EOFException, IOException {
        final int recs;
        byte[] result;
        if (params.getRecordsPerReader() > 0 && params.getRecordsPerReader() > cnt) {
            recs = Math.min(params.getRecordsPerReader() - cnt, params.getRecordsPerSync());
        } else {
            recs =  params.getRecordsPerSync();
        }
        if (cursor == null) {
            cursor = databaseCollection.find().iterator();
        }
        int i = 0;
        status.bytes = 0;
        status.records = 0;
        status.startTime = 0;
        while (cursor.hasNext() && i < recs) {
            key++;
            Binary bin = cursor.next().get("data", org.bson.types.Binary.class);
            result = bin.getData();
            if (result != null) {
                status.bytes += result.length;
                status.records += 1;
                if (status.startTime == 0) {
                    status.startTime = dType.getTime(result);
                }
            }
            i++;
        }
        if (status.records == 0) {
            throw new EOFException();
        }
        status.endTime = System.currentTimeMillis();
        key += status.records;
        cnt += status.records;
        recordTime.accept(id, status.startTime, status.endTime, status.bytes, status.records);
    }

}