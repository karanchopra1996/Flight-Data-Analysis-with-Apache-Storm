package spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import utility.FlightInformation;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FlightsDataReader extends BaseRichSpout {
    private SpoutOutputCollector collector; // for emitting the output tuples
    private FileReader fileReader;
    private boolean completed = false;

    public void ack(Object msgId) {
        System.out.println("OK:" + msgId);
    }

    public void close() {
    }

    public void fail(Object msgId) {
        System.out.println("FAIL:" + msgId);
    }

    public void nextTuple() {
        /**
         * The nextTuple() method is called repeatedly, so if we have already
         * completed processing the data, we will wait for some time and then return.
         */
        if (completed) {
            try {
                Thread.sleep(10000); // Wait for 10 seconds.
            } catch (InterruptedException e) {
                // Do nothing if we get interrupted while waiting.
            }
            return;
        }
        try {
            // Create a list of FlightInformation objects by calling the creatingFlightList() method.
            List<FlightInformation> fInfo = creatingFlightList();
            // Loop through the FlightInformation objects in the list and emit a tuple for each one.
            for (FlightInformation individualFlightDetails : fInfo) {
                // Emit a tuple with the following values.
                this.collector.emit(new Values(
                        individualFlightDetails.getTransponderAddress(),
                        individualFlightDetails.getCallSign(),
                        individualFlightDetails.getOriginCountry(),
                        individualFlightDetails.getStartTimestamp(),
                        individualFlightDetails.getLastTimestamp(),
                        individualFlightDetails.getLongitude(),
                        individualFlightDetails.getLatitude(),
                        individualFlightDetails.getAltitude(),
                        individualFlightDetails.getIsSurface(),
                        individualFlightDetails.getVelocity(),
                        individualFlightDetails.getDegree(),
                        individualFlightDetails.getVerticalRate(),
                        individualFlightDetails.getSensors(),
                        individualFlightDetails.getAltitudeGeometric(),
                        individualFlightDetails.getTransponderCode(),
                        individualFlightDetails.getIsSpecialPurpose(),
                        individualFlightDetails.getOrigin()
                ));
            }
        } catch (Exception e) {
            // Throw a RuntimeException if there is an error reading the tuple.
            throw new RuntimeException("Error reading tuple", e);
        } finally {
            // Set completed to true to indicate that we have finished processing the data.
            completed = true;
        }
    }
    /**
     * We will create the file and get the collector object
     * called when the spout task is initialized
     */
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        try {
            this.fileReader = new FileReader(conf.get("FlightsFile").toString());
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Error reading file [" + conf.get("FlightsFile") + "]");
        }
        this.collector = collector; // collector initialized
    }

    // This method declares the output fields for the Bolt component
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Declare the output fields as a list of Strings
        declarer.declare(new Fields("transponderAddress",
                "callSign",
                "originCountry",
                "firstTimestamp",
                "lastTimestamp",
                "longitude",
                "latitude",
                "altitudeBarometric",
                "SurfaceOrAir",
                "velocity",
                "degreeNorth",
                "verticalRate",
                "sensors",
                "altitudeGeometric",
                "transponderCode",
                "specialPurpose",
                "origin"));
    }

    private List<FlightInformation> creatingFlightList() {
        // Create a JSONParser object
        JSONParser parser = new JSONParser();

// Create an empty list to store FlightInformation objects
        List<FlightInformation> fi = new ArrayList<>();
        try {
            // Parse the JSON file
            Object obj = parser.parse(fileReader);
            // Cast the parsed object to a JSONObject
            JSONObject jsonObject = (JSONObject) obj;
            // Extract the "states" array from the JSONObject
            JSONArray flightList = (JSONArray) jsonObject.get("states");
            // Iterate over each element in the "states" array
            for (Object o : flightList) {
                // Extract the inner JSONArray from the "states" array
                JSONArray innerArray = (JSONArray) o;
                // Create a new FlightInformation object using the values from the inner JSONArray
                FlightInformation flightDetails = new FlightInformation(
                        String.valueOf(innerArray.get(0)),  // transponder
                        String.valueOf(innerArray.get(1)),  // call sign
                        String.valueOf(innerArray.get(2)),  // origin country
                        String.valueOf(innerArray.get(3)),  // First timestamp
                        String.valueOf(innerArray.get(4)),  // Last timestamp
                        String.valueOf(innerArray.get(5)),  // longitude
                        String.valueOf(innerArray.get(6)),  // latitude
                        String.valueOf(innerArray.get(7)),  // altitude
                        String.valueOf(innerArray.get(8)),  // isSurface
                        String.valueOf(innerArray.get(9)),  // velocity
                        String.valueOf(innerArray.get(10)), // degree
                        String.valueOf(innerArray.get(11)), // vertical rate
                        String.valueOf(innerArray.get(12)), // sensors
                        String.valueOf(innerArray.get(13)), // altitude[geometric]
                        String.valueOf(innerArray.get(14)), // transponder code
                        String.valueOf(innerArray.get(15)), // isSpecialpurpose
                        String.valueOf(innerArray.get(16))  // origin
                );
                // Add the FlightInformation object to the list
                fi.add(flightDetails);
            }
        } catch (Exception e) {
            // Print the stack trace if an exception occurs
            e.printStackTrace();
        }
        // Return the list of FlightInformation objects
        return fi;
    }
}




