/**
 * Copyright (c) KMG. All Rights Reserved..
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.sbk.api;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * Interface for Writers.
 */
public interface Writer<T>  {

    /**
     * Asynchronously Writes the data .
     * @param data data to write
     * @return CompletableFuture completable future. null if the write completed synchronously .
     * @throws IOException If an exception occurred.
     */
    CompletableFuture<?> writeAsync(T data) throws IOException;

    /**
     * Flush / Sync the  data.
     * @throws IOException If an exception occurred.
     */
    void sync() throws IOException;

    /**
     * Close the  Writer.
     * @throws IOException If an exception occurred.
     */
    void close() throws IOException;

    /**
     * Default implementation for writing data using {@link io.sbk.api.Writer#writeAsync(Object)})} with time
     * If you are intend to NOT use the CompletableFuture returned by {@link io.sbk.api.Writer#writeAsync(Object)}  )}
     * then you can override this method. otherwise, use the default implementation and don't override this method.
     * If you are intend to use your own payload, then also you can use override this method.
     * you can write multiple records with this method.
     *
     * @param dType   Data Type interface
     * @param data  data to writer
     * @param size  size of the data
     * @param status  write status to return
     * @throws IOException If an exception occurred.
     */
    default void writeAsyncTime(DataType<T> dType, T data, int size, Status status) throws IOException {
        status.bytes = size;
        status.records = 1;
        status.startTime = System.currentTimeMillis();
        writeAsync(dType.setTime(data, status.startTime));
    }


    /**
     * Default implementation for writing data using {@link io.sbk.api.Writer#writeAsync(Object)}  )}
     * and recording the benchmark statistics.
     * If you are intend to NOT use the CompletableFuture returned by {@link io.sbk.api.Writer#writeAsync(Object)}  )}
     * then you can override this method. otherwise, use the default implementation and don't override this method.
     * If you are intend to use your own payload, then also you can use override this method.
     * you can write multiple records with this method.
     *
     * @param dType   Data Type interface
     * @param data   data to write
     * @param size  size of the data
     * @param status Write status to return
     * @param recordTime to call for benchmarking
     * @param  id   Identifier for recordTime
     * @throws IOException If an exception occurred.
     */
    default void recordWrite(DataType<T> dType, T data, int size, Status status, RecordTime recordTime, int id) throws IOException {
        CompletableFuture<?> ret;
        status.bytes = size;
        status.records =  1;
        status.startTime = System.currentTimeMillis();
        ret = writeAsync(data);
        if (ret == null) {
            status.endTime = System.currentTimeMillis();
            recordTime.accept(id, status.startTime, status.endTime, size, 1);
        } else {
            final long time =  status.startTime;
            ret.thenAccept(d -> {
                final long endTime = System.currentTimeMillis();
                recordTime.accept(id, time, endTime, size, 1);
            });
        }
    }

    /**
     * Default implementation for writer benchmarking by writing given number of records.
     * sync is invoked after writing all the records.
     *
     * @param writer Writer Descriptor
     * @param dType  Data Type interface
     * @param data  data to write
     * @param size  size of the data
     * @throws IOException If an exception occurred.
     */
    default void RecordsWriter(Worker writer, DataType<T> dType, T data, int size) throws IOException {
        final Status status = new Status();
        int id = writer.id % writer.recordIDMax;
        int i = 0;
        while (i < writer.params.getRecordsCount()) {
            recordWrite(dType, data, size, status, writer.recordTime, id);
            id += 1;
            if (id >= writer.recordIDMax) {
                id = 0;
            }
            i += status.records;
        }
        sync();
    }

    /**
     * Default implementation for writer benchmarking by writing given number of records.
     * sync is invoked after writing given set of records.
     *
     * @param writer Writer Descriptor
     * @param dType  Data Type interface
     * @param data  data to write
     * @param size  size of the data
     * @param rController Rate Controller
     * @throws IOException If an exception occurred.
     */
    default void RecordsWriterSync(Worker writer, DataType<T> dType, T data, int size, RateController rController) throws IOException {
        final Status status = new Status();
        final int recordsCount = writer.params.getRecordsPerWriter();
        int id = writer.id % writer.recordIDMax;
        int cnt = 0;
        rController.start(writer.params.getRecordsPerSec(), System.currentTimeMillis());
        while (cnt < recordsCount) {
            int loopMax = Math.min(writer.params.getRecordsPerSync(), recordsCount - cnt);
            int i = 0;
            while (i < loopMax) {
                recordWrite(dType, data, size, status, writer.recordTime, id);
                id += 1;
                if (id >= writer.recordIDMax) {
                    id = 0;
                }
                i += status.records;
                cnt += status.records;
                rController.control(cnt, status.startTime);
            }
            sync();
        }
    }

    /**
     * Default implementation for writer benchmarking by continuously writing data records for specific time duration.
     * sync is invoked after writing records for given time.
     *
     * @param writer Writer Descriptor
     * @param dType  Data Type interface
     * @param data  data to write
     * @param size  size of the data
     * @throws IOException If an exception occurred.
     */
    default void RecordsWriterTime(Worker writer, DataType<T> dType, T data, int size) throws IOException {
        final Status status = new Status();
        final long startTime = writer.params.getStartTime();
        final long msToRun = writer.params.getSecondsToRun() * Config.MS_PER_SEC;
        int id = writer.id % writer.recordIDMax;
        status.startTime = System.currentTimeMillis();
        while ((status.startTime - startTime) < msToRun) {
            recordWrite(dType, data, size, status, writer.recordTime, id);
            id += 1;
            if (id >= writer.recordIDMax) {
                id = 0;
            }
        }
        sync();
    }

    /**
     * Default implementation for writer benchmarking by continuously writing data records for specific time duration.
     * sync is invoked after writing given set of records.
     *
     * @param writer Writer Descriptor
     * @param dType  Data Type interface
     * @param data  data to write
     * @param size  size of the data
     * @param rController Rate Controller
     * @throws IOException If an exception occurred.
     */
    default void RecordsWriterTimeSync(Worker writer, DataType<T> dType, T data, int size, RateController rController) throws IOException {
        final Status status = new Status();
        final long startTime = writer.params.getStartTime();
        final long msToRun = writer.params.getSecondsToRun() * Config.MS_PER_SEC;
        int id = writer.id % writer.recordIDMax;
        int cnt = 0;
        status.startTime = System.currentTimeMillis();
        long msElapsed = status.startTime - startTime;
        rController.start(writer.params.getRecordsPerSec(), status.startTime);
        while (msElapsed < msToRun) {
            int i = 0;
            while ((msElapsed < msToRun) && (i < writer.params.getRecordsPerSync())) {
                recordWrite(dType, data, size, status, writer.recordTime, id);
                id += 1;
                if (id >= writer.recordIDMax) {
                    id = 0;
                }
                i += status.records;
                cnt += status.records;
                rController.control(cnt,  status.startTime);
                msElapsed = status.startTime - startTime;
            }
            sync();
        }
    }

    /**
     * Default implementation for writing given number of records. No Writer Benchmarking is performed.
     * Write is performed using {@link io.sbk.api.Writer#writeAsync(Object)}  )}.
     * sync is invoked after writing given set of records.
     *
     * @param writer Writer Descriptor
     * @param dType  Data Type interface
     * @param data  data to write
     * @param size  size of the data
     * @param rController Rate Controller
     * @throws IOException If an exception occurred.
     */
    default void RecordsWriterRW(Worker writer, DataType<T> dType, T data, int size, RateController rController) throws IOException {
        final Status status = new Status();
        final int recordsCount = writer.params.getRecordsPerWriter();
        int id = writer.id % writer.recordIDMax;
        int cnt = 0;
        rController.start(writer.params.getRecordsPerSec(), System.currentTimeMillis());
        while (cnt < recordsCount) {
            int loopMax = Math.min(writer.params.getRecordsPerSync(), recordsCount - cnt);
            int i = 0;
            while (i < loopMax) {
                writeAsyncTime(dType, data, size, status);
                id += 1;
                if (id >= writer.recordIDMax) {
                    id = 0;
                }
                i += status.records;
                cnt += status.records;
                rController.control(cnt, status.startTime);
            }
            sync();
        }
    }

    /**
     * Default implementation for writing data records for specific time duration. No Writer Benchmarking is performed.
     * Write is performed using {@link io.sbk.api.Writer#writeAsync(Object)}  )}.
     * sync is invoked after writing given set of records.
     *
     * @param writer Writer Descriptor
     * @param dType  Data Type interface
     * @param data  data to write
     * @param size  size of the data
     * @param rController Rate Controller
     * @throws IOException If an exception occurred.
     */
    default void RecordsWriterTimeRW(Worker writer, DataType<T> dType, T data, int size, RateController rController) throws IOException {
        final Status status = new Status();
        final long startTime = writer.params.getStartTime();
        final long msToRun = writer.params.getSecondsToRun() * Config.MS_PER_SEC;
        int id = writer.id % writer.recordIDMax;
        int cnt = 0;
        status.startTime = System.currentTimeMillis();
        long msElapsed = status.startTime - startTime;
        rController.start(writer.params.getRecordsPerSec(), status.startTime);
        while (msElapsed < msToRun) {
            int i = 0;
            while ((msElapsed < msToRun) && (i < writer.params.getRecordsPerSync())) {
                writeAsyncTime(dType, data, size, status);
                id += 1;
                if (id >= writer.recordIDMax) {
                    id = 0;
                }
                i += status.records;
                cnt += status.records;
                rController.control(cnt,  status.startTime);
                msElapsed = status.startTime - startTime;
            }
            sync();
        }
    }
}
