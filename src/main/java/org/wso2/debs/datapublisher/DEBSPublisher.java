package org.wso2.debs.datapublisher;


import com.google.gson.Gson;
import org.wso2.carbon.databridge.agent.thrift.Agent;
import org.wso2.carbon.databridge.agent.thrift.DataPublisher;
import org.wso2.carbon.databridge.agent.thrift.conf.AgentConfiguration;
import org.wso2.carbon.databridge.agent.thrift.exception.AgentException;
import org.wso2.carbon.databridge.commons.Event;
import org.wso2.carbon.databridge.commons.exception.NoStreamDefinitionExistException;

import java.io.*;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

public class DEBSPublisher {

    public static final String DEBS_DATA_STREAM = "debs_data";
    public static final String VERSION = "1.0.0";

    private static int count = 1000;

    /**
     * id – a unique identifier of the measurement [64 bit unsigned integer value]
     * timestamp – timestamp of measurement (number of seconds since January 1, 1970, 00:00:00 GMT) [32 bit unsigned integer value]
     * value – the measurement [32 bit floating point]
     * property – type of the measurement: 0 for work or 1 for load [boolean]
     * plug_id – a unique identifier (within a household) of the smart plug [32 bit unsigned integer value]
     * household_id – a unique identifier of a household (within a house) where the plug is located [32 bit unsigned integer value]
     * house_id – a unique identifier of a house where the household with the plug is located [32 bit unsigned integer value]
     */

    public static void main(String[] args) throws Exception {
        String server = args[0];
        String username = args[1];
        String password = args[2];
        String file = args[3];
        count = Integer.parseInt(args[4]);

//        String server = "localhost:7611";
//        String username = "admin";
//        String password = "admin";
//        String file = "/data/packs/test/debs/sorted400M.txt";
        count = 10;

        String streamId;

        System.out.println("Initialized DEBS data publisher on " + server + " with " + count + " records to write");

        System.setProperty("javax.net.ssl.trustStore", "./src/main/resources/truststore/client-truststore.jks");
        System.setProperty("javax.net.ssl.trustStorePassword", "wso2carbon");

        AgentConfiguration agentConfiguration = new AgentConfiguration();
        agentConfiguration.setBufferedEventsSize(50000);

        Agent agent = new Agent(agentConfiguration);
        //using datapublisher
        DataPublisher dataPublisher = new DataPublisher("tcp://" + server, username, password, agent);

        String definition = "{" +
                "  'name':'" + DEBS_DATA_STREAM + "'," +
                "  'version':'" + VERSION + "'," +
                "  'nickName': 'DEBSStream'," +
                "  'description': 'DEBS data'," +
                "  'metaData':[" +
                "          {'name':'publisher','type':'STRING'}" +
                "  ]," +
                "  'payloadData':[" +
                "          {'name':'id','type':'STRING'}," +
                "          {'name':'value','type':'FLOAT'}," +
                "          {'name':'property','type':'INT'}," +
                "          {'name':'composite_plug_id','type':'STRING'}" +
                "  ]" +
                "}";

        try {
            streamId = dataPublisher.findStream(DEBS_DATA_STREAM, VERSION);
            //Stream already defined

        } catch (NoStreamDefinitionExistException e) {
            streamId = dataPublisher.defineStream(definition);
            System.out.println("Defining new stream for " + DEBS_DATA_STREAM);
        }

        if (!streamId.isEmpty()) {
            publishEvents(dataPublisher, streamId, file);
        } else {
            System.out.println("No events published.");
        }

        dataPublisher.stop();
    }

    private static void publishEvents(DataPublisher dataPublisher, String streamId, String file) throws FileNotFoundException {

        BufferedReader br;
        FileReader fr;
        String host;

        Gson gson = new Gson();
        if (new File(file).exists()) {
            try {
                if (getLocalAddress() != null) {
                    host = getLocalAddress().getHostAddress();
                } else {
                    host = "localhost"; // Defaults to localhost
                }
                fr = new FileReader(file);
                br = new BufferedReader(fr);
                String line;
                int ctr = 0;

                while ((line = br.readLine()) != null) {
                    String[] data = line.split(",");
                    Object[] payload = new Object[]{
                            data[0], Float.parseFloat(data[2]), Integer.parseInt(data[3]),
                            gson.toJson(new String[]{data[4], data[5], data[6]})
                    };


                    //constructor used: Event(streamID, timeStamp, metaArray, correlationArray, payloadArray)
                    Event event = new Event(streamId, Long.parseLong(data[1]) * 1000, new Object[]{host}, null, payload);
                    dataPublisher.publish(event);
                    ctr++;
                    if (count > 0) {
                        if (ctr == count) {
                            break;
                        }
                    }
                }

                br.close();
                fr.close();

                System.out.println("Published " + ctr + " events.");

            } catch (NumberFormatException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (AgentException e) {
                e.printStackTrace();
            }
        } else {
            throw new FileNotFoundException("target file not found!");
        }
    }

    private static InetAddress getLocalAddress() throws SocketException {
        Enumeration<NetworkInterface> ifaces = NetworkInterface.getNetworkInterfaces();
        while (ifaces.hasMoreElements()) {
            NetworkInterface iface = ifaces.nextElement();
            Enumeration<InetAddress> addresses = iface.getInetAddresses();
            while (addresses.hasMoreElements()) {
                InetAddress addr = addresses.nextElement();
                if (addr instanceof Inet4Address && !addr.isLoopbackAddress()) {
                    return addr;
                }
            }
        }
        return null;
    }
}