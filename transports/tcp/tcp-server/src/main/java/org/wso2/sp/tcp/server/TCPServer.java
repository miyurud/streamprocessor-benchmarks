/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

 //TODO:CHANGE CSV FILE HEADER NAMES

package org.wso2.sp.tcp.server;

import org.HdrHistogram.Histogram;
import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.io.tcp.transport.TCPNettyServer;
import org.wso2.extension.siddhi.io.tcp.transport.callback.StreamListener;
import org.wso2.extension.siddhi.io.tcp.transport.config.ServerConfig;
import org.wso2.extension.siddhi.map.binary.sourcemapper.SiddhiEventConverter;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Test Server for TCP source
 */
public class TCPServer {
    static Logger log = Logger.getLogger(TCPServer.class);
    private static long firstTupleTime = -1;
    private static String logDir = "./tcp-client-results";
    //private static String filteredLogDir = "./filtered-results-tcp-4.0.0-M20";
    private static final int RECORD_WINDOW = 10000;
    private static long eventCountTotal = 0;
    private static long eventCount = 0;
    private static long timeSpent = 0;
    private static long totalTimeSpent = 0;
   // private static long totalExperimentDuration = 0;
    private static long startTime = System.currentTimeMillis();
    private static boolean flag;
    private static long veryFirstTime = System.currentTimeMillis();
    private static Writer fstream = null;
    private static long outputFileTimeStamp;
    private static boolean exitFlag = false;
    private static int sequenceNumber = 0;
    private static final Histogram histogram = new Histogram(2);
    private static final Histogram histogram2 = new Histogram(2);

    /**
     * Main method to start the test Server
     *
     * @param args host and port are passed as args
     */
    public static void main(String[] args) {

       // totalExperimentDuration=60000;
        try {
            File directory = new File(logDir);
            if (!directory.exists()) {
                //check whether that directory is created or not
                if (!directory.mkdir()) {
                    log.error("Error while creating the output directory.");
                }
            }
            sequenceNumber = getLogFileSequenceNumber();
            outputFileTimeStamp = System.currentTimeMillis();
            fstream = new OutputStreamWriter(new FileOutputStream(new File(logDir + "/output-" +
                                                                                   sequenceNumber + "-" +

                                                                                   (outputFileTimeStamp)
                                                                                   + ".csv")
                                                                          .getAbsoluteFile()), StandardCharsets
                                                     .UTF_8);

        } catch (IOException e) {
            log.error("Error while creating statistics output file, " + e.getMessage(), e);
        }
        /*
         * Stream definition:
         * OutStream (houseId int, maxVal float, minVal float, avgVal double);
         */
        final StreamDefinition streamDefinition = StreamDefinition.id("TCP_Benchmark")
                .attribute("iij_timestamp", Attribute.Type.LONG)
                .attribute("value", Attribute.Type.FLOAT);


        final Attribute.Type[] types = new Attribute.Type[]{Attribute.Type.LONG,
                                                            Attribute.Type.FLOAT};
        TCPNettyServer tcpNettyServer = new TCPNettyServer();
//        tcpNettyServer.addStreamListener(new LogStreamListener("UsageStream"));
//        tcpNettyServer.addStreamListener(new StatisticsStreamListener(streamDefinition));
        tcpNettyServer.addStreamListener(new StreamListener() {

            public String getChannelId() {
                return streamDefinition.getId();
            }

            public void onMessage(byte[] message) {
                onEvents(SiddhiEventConverter.toConvertToSiddhiEvents(ByteBuffer.wrap(message), types));
            }

            public void onEvents(Event[] events) {


                for (Event event : events) {
                    long currentTime = System.currentTimeMillis();

                    if (firstTupleTime == -1) {
                        firstTupleTime = currentTime;
                    }

                    //TODO:Check percentile value
                    long iijTimestamp = Long.parseLong(event.getData()[0].toString());
                    try {
                        eventCount++;
                        eventCountTotal++;
                        timeSpent += (currentTime - iijTimestamp);
                        

                        if (eventCount % RECORD_WINDOW == 0) {
                            totalTimeSpent += timeSpent;
                            long value = currentTime - startTime;

                            if (value == 0) {
                                value++;

                            }

			    histogram2.recordValue((timeSpent)/eventCount);
                            histogram.recordValue((totalTimeSpent) / eventCountTotal);

                            if (!flag) {
                                flag = true;
                                fstream.write("Id, Throughput in this window (events/second), Entire throughput " +
                                                      "for the run (events/second), Total elapsed time(s), Average "
                                                      + "latency "
                                                      +
                                                      "per event in this window(ms), Entire Average latency per event for the run(ms), Total "
                                                      + "number"
                                                      + " of "
                                                      +
                                                      "events received (non-atomic),"+ "AVG latency from start (90),"
                                                      + "" + "AVG latency from start(95), " + "AVG latency from start "
                                                      + "(99)," + "AVG latency in this "
                                                      + "window(90)," + "AVG latency in this window(95),"
                                                      + "AVG latency "
                                                      + "in this window(99)");
                                fstream.write("\r\n");
                            }

                            fstream.write(
                                    (eventCountTotal / RECORD_WINDOW) + "," + ((eventCount * 1000) / value) + "," +
                                            ((eventCountTotal * 1000) / (currentTime - veryFirstTime)) + "," +
                                            ((currentTime - veryFirstTime) / 1000f) + "," + (timeSpent * 1.0
                                            / eventCount) +
                                            "," + ((totalTimeSpent * 1.0) / eventCountTotal) + "," +
                                            eventCountTotal+ "," + histogram.getValueAtPercentile(90.0) + "," + histogram
                                            .getValueAtPercentile(95.0) + "," + histogram.getValueAtPercentile(99.0) + ","
                                            + "" + histogram2.getValueAtPercentile(90.0) + ","
                                            + "" + histogram2.getValueAtPercentile(95.0) + ","
                                            + "" + histogram2.getValueAtPercentile(99.0));
                            fstream.write("\r\n");
                            fstream.flush();
                            histogram2.reset();

                            startTime = System.currentTimeMillis();
                            eventCount = 0;
                            timeSpent = 0;

                            if (!exitFlag) {
                                log.info("Exit flag set");
                                setCompletedFlag(sequenceNumber);
                                exitFlag = true;
                                //dataLoader.shutdown();
                                //siddhiAppRuntime.shutdown();

                            }

                        }
                        //log.info(total_number_of_events_received);
                    } catch (Exception ex) {
                        log.error("Error while consuming event" + ex.getMessage(), ex);
                    }
                    // onEvent(event);


                }
                log.info(events.length);
            }

            public void onEvent(Event event) {
                //  log.info("started");
                log.info(event);
            }
        });

        //preprocessPerformanceData();
        log.info("Done the experiment. Exiting the benchmark");


        ServerConfig serverConfig = new ServerConfig();
        serverConfig.setHost("localhost");
        serverConfig.setPort(Integer.parseInt("9893"));

        tcpNettyServer.start(serverConfig);
        try {
            log.info("Server started, it will shutdown in 100000 millis.");
            Thread.sleep(10000000);
        } catch (InterruptedException e) {
        } finally {
            tcpNettyServer.shutdownGracefully();
        }
    }



    /**
     * This method returns a unique integer that can be used as a sequence number for log files
     */

    private static int getLogFileSequenceNumber() {
        int results = -1;
        BufferedReader br = null;

        //read the flag
        try {
            String sCurrentLine;
            File directory = new File(logDir);

            if (!directory.exists()) {
                if (!directory.mkdir()) {
                    log.error("Error while creating the output directory");
                }
            }
            File sequenceFile = new File(logDir + "/sequence-number.txt");
            if (sequenceFile.exists()) {
                br = new BufferedReader(new InputStreamReader(new FileInputStream(logDir + "/sequence-number.txt"),
                                                              Charset.forName("UTF-8")));

                while ((sCurrentLine = br.readLine()) != null) {
                    results = Integer.parseInt(sCurrentLine);
                    break;
                }
            }
        } catch (IOException e) {
            log.error("Error when reading the sequence number from sequence-number.txt" + e.getMessage(), e);
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
            } catch (IOException exception) {
                exception.printStackTrace();
            }
        }

        //write the new flag
        try {
            if (results == -1) {
                results = 0;
            }

            String content = "" + (results + 1); //need to increment for next iteration
            File file = new File(logDir + "/sequence-number.txt");

            //if file doesn't exists, then create it
            if (!file.exists()) {
                boolean fileCreateResults = file.createNewFile();
                if (!fileCreateResults) {
                    log.error("Error when creating sequence-number.txt file.");
                }
            }
            Writer fstream =
                    new OutputStreamWriter(new FileOutputStream(file.getAbsoluteFile()), StandardCharsets.UTF_8);
            fstream.write(content);
            fstream.flush();
            fstream.close();
        } catch (IOException ex) {
            log.error("Error when writing performance information" + ex.getMessage(), ex);
        }
        return results;
    }

    private static int setCompletedFlag(int sequenceNumber) {
        try {
            String content = "" + sequenceNumber;
            File file = new File(logDir + "/completed-number.txt");

            //if file doesn't exists, then create new file
            if (!file.exists()) {
                boolean fileCreateResults = file.createNewFile();
                if (!fileCreateResults) {
                    log.error("Error when creating completed-number.txt file.");
                }
            }

            Writer fstream =
                    new OutputStreamWriter(new FileOutputStream(file.getAbsoluteFile()), StandardCharsets.UTF_8);

            fstream.write(content);
            fstream.flush();
            fstream.close();
        } catch (IOException e) {
            log.error("Error when writing performance information" + e.getMessage(), e);
        }
        return 0;
    }

}
