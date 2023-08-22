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

public class TopologyMain {
    public static void main(String[] args) throws InterruptedException {
        // Create a new topology
        TopologyBuilder builder = new TopologyBuilder();
        long start = System.currentTimeMillis( );
        // Define a spout named "Flights-Data-Reader" that reads data from a file
        builder.setSpout("Flights-Data-Reader", new FlightsDataReader(),1);
        // Define a bolt named "Hub-Identifier" that identifies the hub of the airport and parallelize it with 4 workers
        builder.setBolt("Hub-Identifier", new HubIdentifier(), 1)
                .shuffleGrouping("Flights-Data-Reader");
        // Define a bolt named "Airline-Sorter" that sorts the airline data by city and parallelize it with 2 workers
        builder.setBolt("Airline-Sorter", new AirlineSorter(), 1)
                .fieldsGrouping("Hub-Identifier", new Fields("airport.city"));

        // Redirect console output logs to a file
        try {
            PrintStream out = new PrintStream(new FileOutputStream("Output.txt"));
            System.setOut(out);
            System.setErr(out);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Unable to redirect console output logs to a file", e);
        }
        // Set configuration options
        Config conf = new Config();
        conf.put("FlightsFile", args[0]); // Set the input file for Flights-Data-Reader
        conf.put("AirportsData", args[1]); // Set the input file for Hub-Identifier
        conf.setDebug(false); // Disable debugging output
        // Run the topology
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1); // Limit the number of pending spouts to 1
        LocalCluster cluster = new LocalCluster(); // Create a local Storm cluster
        cluster.submitTopology("Getting-Started-Topology", conf, builder.createTopology()); // Submit the topology to the cluster
        Thread.sleep(10000); // Sleep for 10 seconds
        long finish = System.currentTimeMillis( );
        long timeElapsed = finish - start;
        System.out.println("Time taken: " + timeElapsed + " ms");
        cluster.shutdown(); // Shut down the cluster

    }
}