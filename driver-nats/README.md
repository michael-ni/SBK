<!--
Copyright (c) KMG. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# NATS Driver for SBK
The NATS driver for SBK supports the End to End latency. This means you have supply both writers and readers while benchmarking.

## NATS standalone server installation 
Refer to this page : https://docs.nats.io/nats-server/installation
or simple just execute the below command
```
 docker run -p 4222:4222 -ti nats:latest
```
you need to have Dockers installed on your system.
An example, SBK benchmarkig command is
```
./build/install/sbk/bin/sbk -class Nats -uri nats://localhost:4222 -topic kmg-topic-1 -size 10 -writers 5 -readers 5 -time 60
```

