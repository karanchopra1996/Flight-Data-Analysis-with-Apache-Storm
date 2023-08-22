package spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

/**
 * @author: Ashish Nagar
 * <p>
 * This class reads flight data from a file and emits it as a tuple.
 */
public class FlightsDataReader extends BaseRichSpout {
    private final int INTERVAL = 10000; // 10 seconds
    private static final String ANSI_GREEN_BACKGROUND = "\u001B[42m";
    private static final String ANSI_YELLOW = "\u001B[33m";
    private static final String ANSI_RESET = "\u001B[0m";
    private SpoutOutputCollector collector;
    private FileReader fileReader;
    private boolean completed = false;

    /**
     * This method `colorPrint` is a utility function that prints the given message to the console with a colored green
     * background and yellow foreground. The method uses ANSI escape codes to set the background and foreground colors.
     */
    public static void colorPrint(String message) {
        System.out.println(ANSI_GREEN_BACKGROUND + ANSI_YELLOW + message + ANSI_RESET);
    }

    @Override
    public void ack(Object msgId) {
        colorPrint("OK:" + msgId);
    }

    @Override
    public void close() {
        if (this.fileReader != null) {
            try {
                this.fileReader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        colorPrint("Spout: cleanup done.");
    }

    @Override
    public void fail(Object msgId) {
        colorPrint("FAIL:" + msgId);
    }

    /**
     * The nextTuple method reads flight data from a file and emits it as a tuple, while sleeping for 10 seconds if the
     * reading is completed.
     */
    public void nextTuple() {
        if (this.completed) {
            try {
                colorPrint("Sleeping for 10 seconds...");
                Thread.sleep(INTERVAL); // 10 seconds
            } catch (InterruptedException e) {
                //Do nothing
            }
        }
        else {
            try (BufferedReader reader = new BufferedReader(this.fileReader)) {
                Values values = null;
                while ((values = this.readFlightData(reader)) != null) {
                    this.collector.emit(values);
                }
            } catch (Exception e) {
                throw new RuntimeException("Error reading tuple", e);
            } finally {
                this.completed = true;
            }
        }
    }

    /**
     * This method reads flight data from a BufferedReader object, skipping lines that start with specific characters,
     * and returns a Values object populated with the next 17 fields of flight data. If there is no more flight data, it
     * returns null.
     *
     * @param reader BufferedReader object.
     *
     * @return Values object with the flight data.
     */
    private Values readFlightData(BufferedReader reader) {
        String line;
        try {
            while ((line = reader.readLine()) != null) {
                line = line.trim().replaceAll("\\s+", ""); // Remove all white spaces.
                // Skip lines that start with specific characters.
                if (!(line.startsWith("{") || line.startsWith("\"states") || line.equals("[") || line.startsWith("]") || line.startsWith("\"time\""))) {
                    break;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Error reading flight data", e);
        }
        String[] flightData = new String[17]; // 17 fields in the flight data

        for (int i = 0; i < 17; i++) {
            assert line != null;
            line = line.split(",")[0].replaceAll("\\s+", ""); // Remove all white spaces
            String[] words = line.split("\""); // Split the line into words

            // Get the word that contains the flight data
            for (String word : words)
                if (word != null && !word.isEmpty()) {
                    flightData[i] = word;
                    break;
                }

            try {
                line = reader.readLine();
                if (line == null)
                    return null;
            } catch (IOException e) {
                throw new RuntimeException("Error reading flight data", e);
            }
        }

        // Return a Values object populated with the flight data
        return new Values(Arrays.copyOfRange(flightData, 0, 17));
    }

    /**
     * Initialize the FlightsDataReader object.
     *
     * @param conf      Configuration object.
     * @param context   Topology context object.
     * @param collector Spout output collector object.
     */
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        colorPrint("Spout: Reading flight data from " + conf.get("FlightsFile"));
        String fileName = conf.get("FlightsFile").toString();
        try {
            this.fileReader = new FileReader(fileName);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Error reading file [" + conf.get("FlightsFile") + "]", e);
        }
        this.collector = collector; // Set the collector
    }

    /**
     * Declare the output fields.
     *
     * @param declarer Output fields declarer object.
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("transponder address", "call sign", "origin country", "first timestamp",
                "last " + "timestamp", "longitude", "latitude", "altitude (barometric)", "surface or air", "velocity "
                + "(meters/sec)", "degree north = 0", "vertical rate", "sensors", "altitude (geometric)",
                "transponder " + "code", "special purpose", "origin"));
    }
}
