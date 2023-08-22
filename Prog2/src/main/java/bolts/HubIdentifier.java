package bolts;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import utility.AirportInformation;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
public class HubIdentifier extends BaseBasicBolt {
    List<AirportInformation> airportInformation;
    private FileReader fileReader;

    // Cleanup method required by BaseBasicBolt but not used in this implementation
    public void cleanup() {
    }

    // Prepare method that initializes the fileReader and airportInformation list
    public void prepare(Map stormConf, TopologyContext context) {
        try {
            // Opening the file with the path specified in the topology configuration
            this.fileReader = new FileReader(stormConf.get("AirportsData").toString());
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Error reading file [" + stormConf.get("AirportsData") + "]");
        }
        // Creating airportInformation list by calling the creatingAirportInformation() method
        airportInformation = creatingAirportInformation();
    }

    // Execute method that performs the main logic of identifying the hub airports
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        // Retrieving the longitude, latitude, and call sign from the input tuple
        String longitude = input.getStringByField("longitude");
        String latitude = input.getStringByField("latitude");
        String callSign = input.getStringByField("callSign");
        // Removing any leading/trailing whitespace from the call sign
        callSign = callSign.trim();
        // If call sign is not empty or "null", only the first 3 characters are considered
        boolean callSignCheckPass = !callSign.equals("")  &&  !callSign.equals("null");
        if( callSignCheckPass)
        {
            callSign = callSign.substring(0,Math.min(3,callSign.length()));
        }
        // Checking if longitude and latitude are null values
        boolean isLongitudeNull = longitude.equals("null");
        // If longitude and latitude are not null values, hub identification is performed
        boolean isLatitudeNull = latitude.equals("null");
        if (!isLongitudeNull && !isLatitudeNull) {
            // Parsing the longitude and latitude values from strings to Doubles
            double flightLongitude = Double.parseDouble(longitude);
            double flightLatitude = Double.parseDouble(latitude);
            // Iterating through the airportInformation list to identify hub airports
            for (AirportInformation information : airportInformation) {
                // Calculating the longitude and latitude differences between the flight and airport coordinates
                int longitudeChangePerDegree = 45;
                double longitudeCheck = Math.abs((flightLongitude - information.getLongitude()) * longitudeChangePerDegree);
                int latitudeChangePerDegree = 70;
                double latitudeCheck = Math.abs((flightLatitude - information.getLatitude()) * latitudeChangePerDegree);
                if (longitudeCheck <= 20 && latitudeCheck <= 20 && callSignCheckPass) {
                    // Emitting a tuple for each hub airport found
                    collector.emit(
                            new Values(
                                    information.getAirportCity(),
                                    information.getAirportCode(),
                                    callSign));
                }
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("airport.city", "airport.code", "flightCallSign"));
    }

    private List<AirportInformation> creatingAirportInformation() {
        String str;
        //Open the reader
        BufferedReader br = new BufferedReader(fileReader);
        //Read all lines
        List<AirportInformation> ai = new ArrayList<>();
        try {
            while ((str = br.readLine()) != null) {
                String[] airportDetails = str.split(",");
                AirportInformation airport = new AirportInformation(
                        airportDetails[0],
                        airportDetails[1],
                        Double.parseDouble(airportDetails[2]), Double.parseDouble(airportDetails[3]));
                ai.add(airport);
                br.readLine();
            }
        } catch (Exception ignored) {
        }
        return ai;
    }
}




