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

package org.wso2.sp.tcp.client;

import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.io.tcp.transport.TCPNettyClient;
import org.wso2.extension.siddhi.map.binary.sinkmapper.BinaryEventConverter;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Test client for TCP source
 */
public class TCPClient {
    static final int EVENT_COUNT = 100000000;
    static final int BATCH_SIZE = 10;
    static final String STREAM_NAME = "TCP_Benchmark";
    static final Attribute.Type[] TYPES = new Attribute.Type[]{Attribute.Type.LONG, Attribute.Type.FLOAT};
    static final Logger LOG = Logger.getLogger(TCPClient.class);

    /**
     * Main method to start the test client
     *
     * @param args host and port need to be provided as args
     */
    public static void main(String[] args) throws IOException, ConnectionUnavailableException {
        /*
         * Stream definition:
         * SmartHomeData (id string, value float, property bool, plugId int, householdId int, houseId int,
         *      currentTime string)
         */
        TCPNettyClient tcpNettyClient = new TCPNettyClient();
        tcpNettyClient.connect("localhost", Integer.parseInt("9893"));
        LOG.info("TCP client connected");

       
        long iij_timestamp;
        float value;

        int i = 0;
        for (; i < EVENT_COUNT; i += BATCH_SIZE) {
            ArrayList<Event> arrayList = new ArrayList<Event>(BATCH_SIZE);
            for (int j = 0; j < BATCH_SIZE; j++) {
	
		//Random rand=new Random();
		iij_timestamp=System.currentTimeMillis();
               // LOG.info(iij_timestamp);
                value = ThreadLocalRandom.current().nextFloat();
		//LOG.info(value);
                arrayList.add(new Event(System.currentTimeMillis(), new Object[]{iij_timestamp,
                        value}));
            }
	    LOG.info(arrayList);
            tcpNettyClient.send(STREAM_NAME, BinaryEventConverter.convertToBinaryMessage(
                    arrayList.toArray(new Event[0]), TYPES).array());
        }
        LOG.info("TCP client finished sending events");
        try {
        LOG.info("TCP client finished sending events");
            Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
        tcpNettyClient.disconnect();
        tcpNettyClient.shutdown();
    }

    private static String getCurrentTimestamp() {
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();
        return dateFormat.format(date);
    }
}
