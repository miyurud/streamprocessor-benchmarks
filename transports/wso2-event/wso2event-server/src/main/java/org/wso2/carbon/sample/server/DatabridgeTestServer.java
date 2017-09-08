/*
*  Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.wso2.carbon.sample.server;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import java.util.List;
import org.apache.log4j.Logger;
import org.wso2.carbon.databridge.commons.Credentials;
import org.wso2.carbon.databridge.commons.Event;
import org.wso2.carbon.databridge.commons.StreamDefinition;
import org.wso2.carbon.databridge.commons.exception.MalformedStreamDefinitionException;
import org.wso2.carbon.databridge.commons.utils.EventDefinitionConverterUtils;
import org.wso2.carbon.databridge.core.AgentCallback;
import org.wso2.carbon.databridge.core.DataBridge;
import org.wso2.carbon.databridge.core.Utils.AgentSession;
import org.wso2.carbon.databridge.core.definitionstore.InMemoryStreamDefinitionStore;
import org.wso2.carbon.databridge.core.exception.DataBridgeException;
import org.wso2.carbon.databridge.core.exception.StreamDefinitionStoreException;
import org.wso2.carbon.databridge.core.internal.authentication.AuthenticationHandler;
import org.wso2.carbon.databridge.receiver.binary.conf.BinaryDataReceiverConfiguration;
import org.wso2.carbon.databridge.receiver.binary.internal.BinaryDataReceiver;
import org.wso2.carbon.databridge.receiver.thrift.ThriftDataReceiver;


/**
 * Databridge Thrift Server which accepts Thrift/Binary events
 */
public class DatabridgeTestServer {
    private static final String STREAM_NAME = "org.wso2.esb.MediatorStatistics";
    private static final String VERSION = "1.3.0";
    private static final Logger log = Logger.getLogger(DatabridgeTestServer.class);
    private static long firstTupleTime = -1;
    private static String logDir = "./wso2events-client-results";
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
    //private static final Histogram histogram = new Histogram(2);
    //private static final Histogram histogram2 = new Histogram(2);
    private ThriftDataReceiver thriftDataReceiver;
    BinaryDataReceiver binaryDataReceiver;
    private InMemoryStreamDefinitionStore streamDefinitionStore;

    private static final String STREAM_DEFN = "{'name':'org.wso2.esb.MediatorStatistics', 'version':'1.3.0', "
            + "'nickName':'Stock Quote Information', 'description':'Some Desc', "
            + "'payloadData':[{'name':'iij_timestamp','type':'LONG'}, {'name':'value','type':'FLOAT'}]}";

    public static void main(String args[]) throws DataBridgeException, InterruptedException,
                                                  StreamDefinitionStoreException, MalformedStreamDefinitionException {
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

        DatabridgeTestServer databridgeTestServer = new DatabridgeTestServer();
        databridgeTestServer.addStreamDefinition(STREAM_DEFN);
        databridgeTestServer.start(args[0], Integer.parseInt(args[1]), args[2]);
        Thread.sleep(100000000);
        databridgeTestServer.stop();
    }


    public void addStreamDefinition(String streamDefinitionStr)
            throws StreamDefinitionStoreException, MalformedStreamDefinitionException {
        StreamDefinition streamDefinition = EventDefinitionConverterUtils.convertFromJson(streamDefinitionStr);
        getStreamDefinitionStore().saveStreamDefinitionToStore(streamDefinition, -1);
    }

    private InMemoryStreamDefinitionStore getStreamDefinitionStore() {
        if (streamDefinitionStore == null) {
            streamDefinitionStore = new InMemoryStreamDefinitionStore();
        }
        return streamDefinitionStore;
    }

    public void start(String host, int receiverPort, String protocol) throws DataBridgeException {
        WSO2EventServerUtil.setKeyStoreParams();
        streamDefinitionStore = getStreamDefinitionStore();
        DataBridge databridge = new DataBridge(new AuthenticationHandler() {

            public boolean authenticate(String userName,
                                        String password) {
                return true; // allays authenticate to true

            }

            public void initContext(AgentSession agentSession) {
                //To change body of implemented methods use File | Settings | File Templates.
            }

            public void destroyContext(AgentSession agentSession) {

            }

            public int getTenantId(String id) {
                return -1;
            }

            public String getTenantDomain(String id) {
                return null;
            }
        }, streamDefinitionStore, WSO2EventServerUtil.getDataBridgeConfigPath());

        thriftDataReceiver = new ThriftDataReceiver(receiverPort, databridge);

        databridge.subscribe(new AgentCallback() {

            //public void definedStream(StreamDefinition streamDefinition) {
           //     log.info("StreamDefinition " + streamDefinition);
           // }

            public void definedStream(StreamDefinition streamDefinition, int value) {
                log.info("StreamDefinition with int parameter " + streamDefinition);
            }

            //public void removeStream(StreamDefinition streamDefinition) {
            //    log.info("StreamDefinition remove " + streamDefinition);
            //}

            public void removeStream(StreamDefinition streamDefinition, int value) {
                log.info("StreamDefinition remove with int parameter " + streamDefinition);
            }

            public void receive(List<Event> events, Credentials credentials) {

                for (Event evt : events) {
                    long currentTime = System.currentTimeMillis();

                    if (firstTupleTime == -1) {
                        firstTupleTime = currentTime;
                    }

                    //log.info(evt.getPayloadData());
                    //  Event[] payLoadDataArray;
                    //  for(Event e:payLoadDataArray){
                    //
                    //  }
                    //log.info();
                    long iijTimestamp=Long.parseLong(evt.getPayloadData()[0].toString());
                    log.info("Time is"+" "+iijTimestamp);
                    //  String[] a = events.toString().split("payloadData");
                    //  String[] b = a[1].split(",");
                    //  String[] c = b[0].split("\\[");
                    //  String x = c[1];
                    //  long iijTimestamp = Long.parseLong(x);
                    try {
                        eventCount++;
                        eventCountTotal++;
                        timeSpent += (currentTime - iijTimestamp);
                        log.info("event count is" + eventCount);


                        if (eventCount % RECORD_WINDOW == 0) {
                            totalTimeSpent += timeSpent;
                            long value = currentTime - startTime;

                            if (value == 0) {
                                value++;

                            }
                            log.info(eventCount);

                            if (!flag) {

                                flag = true;
                                fstream.write("Id, Throughput in this window (events/second), Entire throughput " +
                                                      "for the run (events/second), Total elapsed time(s), Average "
                                                      + "latency "
                                                      +
                                                      "per event (ms), Entire Average latency per event (ms), Total "
                                                      + "number"
                                                      + " of "
                                                      +
                                                      "events received (non-atomic)," + "AVG latency from start (90),"
                                                      + "" + "AVG latency from start(95), " + "AVG latency from start "
                                                      + "(99)," + "AVG latency in this "
                                                      + "window(90)," + "AVG latency in this window(95),"
                                                      + "AVG latency "
                                                      + "in this window(99)");
                                fstream.write("\r\n");
                            }
                            //log.info("sample"+eventCountTotal/RECORD_WINDOW);
                            fstream.write(
                                    (eventCountTotal / RECORD_WINDOW) + "," + ((eventCount * 1000) / value) + "," +
                                            ((eventCountTotal * 1000) / (currentTime - veryFirstTime)) + "," +
                                            ((currentTime - veryFirstTime) / 1000f) + "," + (timeSpent * 1.0
                                            / eventCount) +
                                            "," + ((totalTimeSpent * 1.0) / eventCountTotal) + "," +
                                            eventCountTotal);
                            fstream.write("\r\n");
                            fstream.flush();
                            //histogram2.reset();

                            startTime = System.currentTimeMillis();
                            eventCount = 0;
                            timeSpent = 0;

                            if (!exitFlag) {
                                log.info("Exit flag set");
                                setCompletedFlag(sequenceNumber);
                                exitFlag = true;


                            }

                        }
                        //log.info(total_number_of_events_received);
                    } catch (Exception ex) {
                        log.error("Error while consuming event" + ex.getMessage(), ex);
                    }
                    // onEvent(event);


                    // for(String c:b){
                    // log.info("c is"+c);
                    // }

                    log.info("events are"+evt);
                    //log.info("eventListSize=" + events.size() + " eventList " + evt + " for username " +
                                    //  credentials.getUsername());

                }
            }

        });


        if (protocol.equalsIgnoreCase("binary")) {
            binaryDataReceiver = new BinaryDataReceiver(new BinaryDataReceiverConfiguration(receiverPort + 100,
                                                                                            receiverPort), databridge);
            try {
                binaryDataReceiver.start();
            } catch (IOException e) {
                log.error("Error occurred when reading the file : " + e.getMessage(), e);
            }
        } else {
            thriftDataReceiver = new ThriftDataReceiver(receiverPort, databridge);
            thriftDataReceiver.start(host);
        }

        log.info("Test Server Started");
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

    public void stop() {
        thriftDataReceiver.stop();
        log.info("Test Server Stopped");
    }


}
