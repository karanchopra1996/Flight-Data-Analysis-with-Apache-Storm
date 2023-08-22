package bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import utility.AirportInformation;

import java.io.FileReader;
import java.util.*;

public class AirlineSorter extends BaseBasicBolt {
    // Declare variables
    Integer id;
    String name;
    Map<String, Map<String, Integer>> counters;
    List<AirportInformation> airportInformation;
    private FileReader fileReader;
    // Define a method to clean up data after the bolt has finished executing

    public void cleanup() {
        // sort the flight count per airport
        for(Map.Entry<String, Map<String,Integer>> en : counters.entrySet()){
            Map<String, Integer> sortedAirLines = sortByValue(en.getValue());
            System.out.println(en.getKey());
            counters.put(en.getKey(),sortedAirLines);
        }
        // Loop through each entry in the counters map
        for (Map.Entry<String, Map<String, Integer>> temp : counters.entrySet()) {
            int totalFlights = 0;
            // Print the name of the airport
            System.out.println("At Airport: " + temp.getKey());
            // Loop through each entry in the inner map and print the flight code and its count
            for (Map.Entry<String, Integer> innerMap : temp.getValue().entrySet()) {
                System.out.println(innerMap.getKey() + ":" + innerMap.getValue());
                totalFlights = totalFlights + innerMap.getValue();
            }
            // Print the total number of flights for the current airport
            System.out.println("total # flights = " + totalFlights);
            System.out.println();
        }

    }
    //Method to sort the hashmap on the basis of value
    public static Map<String, Integer> sortByValue(Map<String, Integer> hm)
    {
        // Create a list from elements of HashMap
        List<Map.Entry<String, Integer> > list = new LinkedList<>(hm.entrySet());
        // Sort the list
        list.sort((o1, o2) -> (o2.getValue()).compareTo(o1.getValue()));
        // put data from sorted list to hashmap
        HashMap<String, Integer> temp = new LinkedHashMap<>();
        for (Map.Entry<String, Integer> aa : list) {
            temp.put(aa.getKey(), aa.getValue());
        }
        return temp;
    }
    // Define a method to set up the bolt before it starts executing
    public void prepare(Map stormConf, TopologyContext context) {
        // Initialize the counters map
        this.counters = new HashMap<String, Map<String, Integer>>();
        // Get the name and ID of the current bolt
        this.name = context.getThisComponentId();
        this.id = context.getThisTaskId();
    }
    // Define a method to declare the output fields of the bolt
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
    // Define the main method for the bolt, which processes tuples of data
    public void execute(Tuple input, BasicOutputCollector collector) {
        // Extract the airport city, airport code, and flight code from the input tuple
        String airportCity = input.getStringByField("airport.city");
        String airportCode = input.getStringByField("airport.code");
        String flightCode = input.getStringByField("flightCallSign");
        // Create a key to represent the airport, which includes the airport code and city
        String key = airportCode + "(" + airportCity + ")";
        // Check if the counters map already contains the current airport
        if (counters.containsKey(key)) {
            // If it does, update the existing inner map with the new flight code and count
            Map<String, Integer> temp = counters.get(key);
            temp.put(flightCode, temp.getOrDefault(flightCode, 0) + 1);
            //counters.put(airportCode, temp);
        } else {
            // If it doesn't, create a new inner map and add the current flight code and count to it
            Map<String, Integer> temp = new HashMap<>(); // create a new hashmap
            temp.put(flightCode, temp.getOrDefault(flightCode, 0) + 1);
            counters.put(key, temp);
        }
    }
}
