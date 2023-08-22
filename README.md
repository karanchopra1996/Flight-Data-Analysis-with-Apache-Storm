# Flight-Data-Analysis-with-Apache-Storm
Practices distributed data streaming and analysis, using Apache Storm


As an example
dataset, we will use the Open Air Traffic Data for Research website (at https://opensky-network.org/) and
count the number of flights departing from or arriving at each US major airport per airline company.

TopologyMain.java: Created a Storm topology that reads flight data from a file and performs two operations on it: identifying the hub of each airport and sorting the airline data by city. 
The topology consists of a single spout ("Flights-Data-Reader") that reads data from a file, a bolt ("Hub-Identifier") that identifies the hub of each airport, 
and a bolt ("Airline-Sorter") that sorts the airline data by city. 
The output logs of the program are redirected to a file. 
The topology is submitted to a local Storm cluster and runs for 10 seconds. 
The program also measures the time elapsed during the execution of the topology and prints it to the console.



FlightsDataReader.java: Defines a Storm spout, which reads flight data from a JSON file and emits a tuple for each flight in the file. 
The FlightsDataReader class extends the BaseRichSpout class and implements its methods to initialize the spout, declare its output fields, and emit tuples. 
The spout reads flight data from a file, and parses the JSON data using the json.simple library, and creates a FlightInformation object for each flight.
The FlightInformation objects are emitted as tuples with the following fields: transponderAddress, callSign, originCountry, firstTimestamp, lastTimestamp, longitude, latitude, altitudeBarometric, SurfaceOrAir, velocity, degreeNorth, verticalRate, sensors, altitudeGeometric, transponderCode, specialPurpose, and origin. Once all flights have been emitted, the spout waits for 10 seconds before exiting.



HubIdentifier.java: Implements a Bolt in a Storm topology for identifying hub airports based on the location data of incoming flight data. 
The Bolt reads a CSV file containing airport information and creates a list of objects that store this data. 
The Bolt then receives incoming tuples containing longitude, latitude, and call sign data for each flight. 
It checks whether the longitude and latitude data are not null and then iterates through the list of airports, 
calculating the distance between each airport and the incoming flight. 
If the distance is within a certain threshold, and the call sign is not empty or "null", 
the Bolt emits a tuple containing the airport city, airport code, and flight call sign.


AirlineSorter.java: The AirlineSorter class is a bolt in a Storm topology that processes tuples of data. 
It declares several variables, including an ID, a name, a map called counters that contains an inner map of flight codes and their counts for each airport, 
a list of AirportInformation, and a FileReader.
The class implements several methods including cleanup(), prepare(), declareOutputFields(), and execute(). 
The cleanup() method sorts the flight count per airport in the counters map by calling the sortByValue() method and then prints the airport name, flight codes, 
and a total number of flights for each airport. 
The sortByValue() method sorts a hashmap by value and returns the sorted hashmap. 
The prepare() method initializes the counters map and gets the name and ID of the bolt. 
The declareOutputFields() method declares the output fields of the bolt.
The execute() method extracts the airport city, airport code, and flight code from the input tuple, creates a key to represent the airport, checks if the counters map already contains the current airport, and either updates the existing inner map with the new flight code and the count or creates a new inner map and adds the current flight code and count to it.


Two additional files are created which are used in the above-mentioned java class files, the description of the files are below:
FlightInformation.java: Defines a Java class called FlightInformation that implements the Serializable interface. It has several private member variables such as transponderAddress, callSign, originCountry, startTimestamp, etc. It also has a constructor that initializes all these member variables and several getters and setter methods to access and modify these member variables. This class is used to store information about flights. AirportInformation.java: The code defines a Java class named "AirportInformation" with fields for airport city, code, latitude, and longitude. It also includes getter and setter methods for these fields and an overridden toString() method for printing the values of the fields. The class implements the Serializable interface to allow for object serialization.
Added an extra additional feature where I sorted the call signs. Sorts a Map<String, Integer> by its values in descending order. It first converts the input Map to a List of Map. Entry objects using LinkedList. Then, it sorts the list using a lambda expression that compares the values of each Map. Entry objects in descending order. After that, it creates a new LinkedHashMap object to store the sorted entries. It then iterates through the sorted list using a for-each loop and puts each Map.Entry object into the LinkedHashMap using the put method. Finally, the sorted Map<String, Integer> is returned. This method is a concise and efficient way to sort a Map by its values.

