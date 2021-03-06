/**
 * Copyright (c) KMG. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.sbk.api.impl;

import io.sbk.api.DataType;
import io.sbk.api.Parameters;
import io.sbk.api.RecordTime;
import io.sbk.api.Reader;
import io.sbk.api.Worker;

import java.io.EOFException;
import java.io.IOException;

/**
 * Reader Benchmarking Implementation.
 */
public class SbkReader extends Worker implements RunBenchmark {
    final private DataType<Object> dType;
    final private Reader<Object> reader;
    final private RunBenchmark perf;

    public SbkReader(int readerId, int idMax, Parameters params, RecordTime recordTime, DataType<Object> dType, Reader<Object> reader) {
        super(readerId, idMax, params, recordTime);
        this.dType = dType;
        this.reader = reader;
        this.perf = createBenchmark();
    }

    @Override
    public void run() throws EOFException, IOException {
        perf.run();
     }

    private RunBenchmark createBenchmark() {
        final RunBenchmark perfReader;
        if (params.getSecondsToRun() > 0) {
            perfReader = params.isWriteAndRead() ? this::RecordsTimeReaderRW : this::RecordsTimeReader;
        } else {
            perfReader = params.isWriteAndRead() ? this::RecordsReaderRW : this::RecordsReader;
        }
        return perfReader;
    }

    private void RecordsReader() throws EOFException, IOException {
        reader.RecordsReader(this, dType);
    }


    private void RecordsReaderRW() throws EOFException, IOException {
        reader.RecordsReaderRW(this, dType);
    }

    private void RecordsTimeReader() throws EOFException, IOException {
        reader.RecordsTimeReader(this, dType);
    }

    private void RecordsTimeReaderRW() throws EOFException, IOException {
        reader.RecordsTimeReaderRW(this, dType);
    }
}
