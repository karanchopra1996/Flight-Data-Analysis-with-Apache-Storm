package bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author: Ashish Nagar
 * <p>
 * This class reads airport data from a file and emits a tuple with the airport city, airport code, and flight.
 */
public class HubIdentifier extends BaseBasicBolt {
    private static final String ANSI_YELLOW_BACKGROUND = "\u001B[43m";
    private static final String ANSI_BLUE = "\u001B[34m";
    private static final String ANSI_RESET = "\u001B[0m";
    private AirportData[] airports = null;

    /**
     * This method `colorPrint` is a utility function that prints the given message to the console with a colored yellow
     * background and blue foreground. The method uses ANSI escape codes to set the background and foreground colors.
     */
    public static void colorPrint(String message) {
        System.out.println(ANSI_YELLOW_BACKGROUND + ANSI_BLUE + message + ANSI_RESET);
    }

    @Override
    public void cleanup() {
        colorPrint("Bolt1: cleanup done.");
    }

    /**
     * Initialize airports by reading airport data from a file specified in the Storm configuration.
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        colorPrint("Bolt1: preparing...");
        this.airports = this.readAirportData(stormConf.get("AirportsData").toString());
    }

    /**
     * This method reads airport data from a file and returns an array of AirportData objects. It checks for lines
     * consisting of four words separated by commas, converts ArrayList to an array, and throws a RuntimeException if an
     * error occurs during file reading.
     */
    private AirportData[] readAirportData(String fileName) {
        //colorPrint("Bolt1: Reading airport data from " + fileName);
        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            List<AirportData> airportDataList = new ArrayList<>(40); // 40 airports
            String line;

            // Read the file line by line
            while ((line = reader.readLine()) != null) {
                String[] words = line.split(","); // Split the line into words
                if (words.length == 4) {
                    // Add a new AirportData object to the list
                    airportDataList.add(new AirportData(words[0], words[1], Double.parseDouble(words[2]),
                            Double.parseDouble(words[3]))); // city, code, latitude, longitude
                }
            }
            // Convert the list to an array and return it
            return airportDataList.toArray(new AirportData[0]);
        } catch (IOException e) {
            throw new RuntimeException("Error reading " + fileName, e);
        }
    }

    /**
     * The execute method receives a tuple from the stream and checks whether the flight is close enough to any of the
     * airports. If it is, the method emits a tuple with the airport city, airport code, and flight.
     */
    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            // Get the flight, latitude, and longitude from the tuple
            String flight = input.getStringByField("call sign");
            double latitude = Double.parseDouble(input.getStringByField("latitude"));
            double longitude = Double.parseDouble(input.getStringByField("longitude"));

            // Check whether the flight is close enough to any of the airports and emit a tuple if it is
            for (AirportData airport : this.airports)
                if (Math.abs(airport.latitude - latitude) <= 0.286 && Math.abs(airport.longitude - longitude) <= 0.444)
                    collector.emit(new Values(airport.city, airport.code, flight));
        } catch (NumberFormatException e) {
            //System.err.println("Invalid input: " + e.getMessage());
        }
    }

    /**
     * The declareOutputFields method declares that the HubIdentifier bolt emits a tuple with three fields:
     * airport.city, airport.code, and flight.
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("airport.city", "airport.code", "flight"));
    }

    /**
     * The AirportData class is a serializable class that represents information about an airport, including its city,
     * airport code, latitude, and longitude. The class has a constructor that initializes the values of these
     * properties, and all properties are marked as final to ensure immutability.
     */
    private static class AirportData implements Serializable {
        private final String city;
        private final String code;
        private final double latitude;
        private final double longitude;

        private AirportData(String city, String code, double latitude, double longitude) {
            this.city = city;
            this.code = code;
            this.latitude = latitude;
            this.longitude = longitude;
        }
    }
}
