import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolts.AirlineSorter;
import bolts.HubIdentifier;
import spouts.FlightsDataReader;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;

/**
 * @author: Ashish Nagar
 * <p>
 * This class is the main class for the airport flights topology.
 */
public class TopologyMain {
    private static final int INTERVAL = 10000; // 10 seconds

    public static void main(String[] args) throws InterruptedException {
        long startTime = System.currentTimeMillis(); // Start time

        // Topology definition
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("flight-data-reader", new FlightsDataReader()); // spout
        builder.setBolt("hub-identifier", new HubIdentifier()).shuffleGrouping("flight-data-reader"); //
        // bolt1
        builder.setBolt("airline-sorter", new AirlineSorter()).fieldsGrouping("hub-identifier",
                new Fields("airport" + ".city")); // bolt2

        // Configuration
        Config conf = new Config();
        conf.put("FlightsFile", args[0]); // flights.txt file
        conf.put("AirportsData", args[1]); // airports.txt file
        conf.setDebug(false);

        // Redirect console output logs to a file
        try {
            PrintStream out = new PrintStream(new FileOutputStream("console.txt", true));
            //System.setOut(out);
            System.setErr(out);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Unable to redirect console output logs to a file", e);
        }

        // Topology run
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1); // To avoid memory issues
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Airport-Flights-Topology", conf, builder.createTopology());

        Thread.sleep(INTERVAL); // Wait for 10 seconds
        cluster.shutdown();

        // Compute and print the running time
        long runningTime = System.currentTimeMillis() - startTime;
        System.out.println("\u001B[41m" + "\u001B[35m" + "Total running time: " + runningTime + " ms" + "\u001B[0m");
    }
}
