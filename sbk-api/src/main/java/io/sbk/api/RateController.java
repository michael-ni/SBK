/**
 * Copyright (c) KMG. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.sbk.api;

/**
 * Interface for Rate or Throughput Controller.
 */
public interface RateController {

    /**
     * Start the Rate Controller.
     *
     * @param recordsPerSec Records Per Second.
     * @param time   start time
     */
    void start(int recordsPerSec, long time);

    /**
     * Blocks for small amounts of time to achieve targetThroughput/events per sec.
     *
     * @param records current records
     * @param time   current time
     */
    void control(long records, long time);

}
