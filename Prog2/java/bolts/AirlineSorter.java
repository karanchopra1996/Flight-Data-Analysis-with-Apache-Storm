package bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;

/**
 * @author: Ashish Nagar
 * <p>
 * This class reads the data emitted by the HubIdentifier bolt and sorts the data by airport code or call signs. It
 * prints the statistics for each airport to the console.
 */
public class AirlineSorter extends BaseBasicBolt {

    Integer id;
    String name;
    private Map<String, String> stringMap;
    private Map<String, Map<String, Integer>> map;
    private static final String ANSI_CYAN_BACKGROUND = "\u001B[46m";
    private static final String ANSI_YELLOW = "\u001B[33m";
    private static final String ANSI_RESET = "\u001B[0m";
    private static final Map<String, String> icaoToAirlineMap = new HashMap<>();
    private static final String WIKIPEDIA_URL = "https://en.wikipedia.org/wiki/List_of_airline_codes";
    private final boolean sortByCallSigns = true; // set to true to sort by call signs
    private final boolean sortByAirportCode = false; // set to true to sort by airports' codes

    /**
     * This method `colorPrint` is a utility function that prints the given message to the console with a colored cyan
     * background and yellow foreground. The method uses ANSI escape codes to set the background and foreground colors.
     */
    public static void colorPrint(String message) {
        System.out.println(ANSI_CYAN_BACKGROUND + ANSI_YELLOW + message + ANSI_RESET);
    }

    /**
     * Sorts the airports and airlines data and prints the statistics for each airport. The sorting can be based on
     * either call signs or number of flights for airlines, and on airport codes for airports. The data is printed to
     * the console in a formatted manner. After printing the statistics, the method clears the maps of airports and
     * airlines data.
     */
    @Override
    public void cleanup() {
        colorPrint("-- Airlines sorted by " + (sortByAirportCode ? "Airports' Codes" : (sortByCallSigns ? "Call " +
                "Signs (# of flights)" : "Airlines (default)")) + " --");
        // Sort the airports if required
        Map<String, Map<String, Integer>> airports = sortByAirportCode ? new TreeMap<>(map) : map;

        // Print the airline data statistics for each airport
        for (Map.Entry<String, Map<String, Integer>> airport : airports.entrySet()) {
            printAirportInfo(sortByCallSigns, airport, stringMap);
        }

        // Clear the maps
        airports.clear();
        map.clear();
        stringMap.clear();
        colorPrint("Bolt2: cleanup done.");
    }

    /**
     * Prints the information of an airport, including the airport name, number of flights and statistics for each
     * airline.
     *
     * @param sortByCallSigns a boolean indicating whether to sort the airlines by call signs.
     * @param airport         a Map.Entry object containing the airport name and its corresponding airline data.
     * @param stringMap       a Map object containing the airport names and their corresponding airport codes.
     */
    private static void printAirportInfo(boolean sortByCallSigns, Map.Entry<String, Map<String, Integer>> airport,
                                         Map<String, String> stringMap) {
        String airportName = airport.getKey();
        colorPrint("At Airport: " + airportName + " (" + stringMap.get(airportName) + ")");
        int numberOfFlights = 0;
        List<Map.Entry<String, Integer>> airlines = new ArrayList<>(airport.getValue().entrySet());

        // Sort airlines if required
        if (sortByCallSigns)
            airlines.sort(Map.Entry.<String, Integer>comparingByValue().reversed());
        String airlineName;
        // Print the statistics for each airline
        for (Map.Entry<String, Integer> airline : airlines) {
            numberOfFlights += airline.getValue();
            airlineName = icaoToAirlineMap.get(airline.getKey());
            airlineName = sortByCallSigns ? (" [" + (airlineName != null ? airlineName + "]" : "No Airline Info!]"))
                    : "";
            colorPrint("\t" + airline.getKey() + ": " + airline.getValue() + airlineName);
        }

        colorPrint("Total #flights => " + numberOfFlights + "\n");
        airlines.clear();
    }

    /**
     * Prepares the AirlineSorter bolt by initializing the necessary variables and printing a log message.
     *
     * @param stormConf the configuration map for the Storm cluster
     * @param context   the context for this bolt within the topology
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        colorPrint("Bolt2: preparing...");
        this.map = new HashMap<>(); // map of airport codes and airline frequencies
        this.stringMap = new HashMap<>(); // map of airport codes and airport names
        this.name = context.getThisComponentId(); // name of the bolt
        this.id = context.getThisTaskId(); // id of the bolt
        try {
            if (sortByCallSigns)
                mapCodesToAirlines(); // map ICAO codes to airline names
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    /**
     * Extracts the airport city and code, and the flight carrier code from the input tuple, and updates the map of
     * airline frequencies for the corresponding airport.
     *
     * @param input     The input tuple containing airport and flight information.
     * @param collector The output collector for emitting tuples to downstream bolts.
     */
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String airportCity = input.getStringByField("airport.city");
        String airportCode = input.getStringByField("airport.code");
        String flight = input.getStringByField("flight");
        if (flight != null) {
            stringMap.computeIfAbsent(airportCode, k -> airportCity); // add airport to map

            Map<String, Integer> airlines = map.computeIfAbsent(airportCode, k -> new HashMap<>()); // get airline map
            String carrier = flight.substring(0, 3); // get carrier code
            airlines.put(carrier, airlines.getOrDefault(carrier, 0) + 1); // update airline frequency
        }
    }

    /**
     * Maps ICAO codes to airline names by scraping data from the Wikipedia page specified by WIKIPEDIA_URL constant.
     * The data is parsed using Jsoup and stored in a HashMap for efficient lookup.
     *
     * @throws IOException if there is an error in connecting to or reading from the Wikipedia page.
     */
    private static void mapCodesToAirlines() throws IOException {
        // URL object for the Wikipedia page containing the ICAO codes and airline names
        URL url = new URL(WIKIPEDIA_URL);

        // Open a connection to the URL and send an HTTP GET request
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        StringBuilder stringBuilder = new StringBuilder();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                stringBuilder.append(line);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            connection.disconnect();
        }

        Document doc = Jsoup.parse(stringBuilder.toString()); // parse the HTML document

        // Find the HTML table element containing the airline codes and names
        Element table = doc.select("table.wikitable.sortable").first();

        // Loop through the rows of the table and extract the values for the ICAO code and Airline columns
        assert table != null;
        Elements rows = table.select("tr");
        for (int i = 1; i < rows.size(); i++) {
            Element row = rows.get(i);
            Elements cols = row.select("td");
            if (cols.size() >= 3) {
                String airlineIcaoCode = cols.get(1).text().trim();
                String airlineName = cols.get(2).text().trim();
                // Add the ICAO code and airline name to the map
                icaoToAirlineMap.put(airlineIcaoCode, airlineName);
            }
        }
    }
}
