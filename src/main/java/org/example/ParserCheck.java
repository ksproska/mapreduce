package org.hadoop;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ParserCheck {

    public static void main(String[] args) throws Exception {
        String path = "src/main/java/org/hadoop/listings_detailed.csv";  //objects 42930
        String pathOut = "src/main/java/org/hadoop/listings_detailed_cleaned.csv";
        String line = "";
        String lineCSV = "801749842377802394,https://www.airbnb.com/rooms/801749842377802394,20230306014634,2023-03-06,city scrape,A home away from home,\"The whole group will be comfortable in this spacious and unique space.<br /><br /><b>Guest access</b><br />you can enjoy the living room, dining room, kitchen and the backyard. The home has two entrances\",,https://a0.muscache.com/pictures/miso/Hosting-801749842377802394/original/bf54da63-4a14-4185-a2e2-919a959249bf.jpeg,495455523,https://www.airbnb.com/users/show/495455523,Michael,2023-01-10,,\"\",N/A,N/A,N/A,f,https://a0.muscache.com/im/pictures/user/773edbb5-05e0-46f9-8a19-1fbcd3d841c3.jpg?aki_policy=profile_small,https://a0.muscache.com/im/pictures/user/773edbb5-05e0-46f9-8a19-1fbcd3d841c3.jpg?aki_policy=profile_x_medium,Canarsie,1,1,\"['email', 'phone']\",t,f,,Canarsie,Brooklyn,40.640402656712844,-73.88853475272201,Private room in home,Private room,2,,1 bath,1,1,\"[\"\"50\\\"\" TV\"\", \"\"Bathtub\"\", \"\"Microwave\"\", \"\"Free driveway parking on premises\"\", \"\"Laundromat nearby\"\", \"\"Kitchen\"\", \"\"Refrigerator\"\", \"\"Central air conditioning\"\", \"\"Smoke alarm\"\", \"\"Central heating\"\", \"\"Free street parking\"\", \"\"Clothing storage: closet and walk-in closet\"\", \"\"Essentials\"\", \"\"Carbon monoxide alarm\"\", \"\"Dining table\"\", \"\"Stainless steel oven\"\", \"\"Wifi\"\", \"\"Ceiling fan\"\", \"\"Dedicated workspace\"\", \"\"Gas stove\"\", \"\"Cooking basics\"\", \"\"Private backyard\"\", \"\"Hot water kettle\"\", \"\"Bed linens\"\", \"\"Hot water\"\"]\",$143.00,2,30,2,2,30,30,2.0,30.0,,t,30,60,90,364,2023-03-06,0,0,0,,,,,,,,,,,f,1,0,1,0,";
        long wrong = 0;
        long counter = 0;
        File csvFile = new File(pathOut);
//        try (FileReader fileReader = new FileReader(csvFile)) {
//            // Using CSVFormat to define how the CSV data should be parsed
//            CSVParser parser = CSVFormat.DEFAULT
//                    .withFirstRecordAsHeader() // Treat the first record as headers, useful for skipping headers
//                    .withQuote('"')            // Define quote character
//                    .withIgnoreSurroundingSpaces(true) // Ignore spaces surrounding quotes
//                    .parse(fileReader);
//            long counter = 0;
//
//            // Iterating over the records and processing each line
//            for (CSVRecord record : parser) {
//                String[] values = new String[record.size()];
//                if (record.size() < 33){
//                    wrong++;
//                    for (int i = 0; i < record.size(); i++) {
//                        values[i] = record.get(i);
//                    }
//                }
//                // Handle or process the extracted data here
//
//                System.out.println(counter);
//                counter++;
//            }
//        }

//        try (BufferedReader br = new BufferedReader(new FileReader(pathOut))) {
//            while ((line = br.readLine()) != null) {
//                String[] values = Utils.parseCSVLine(line);  // Splitting each line with comma
//                // Process each value as needed
//                counter++;
//                if (values.length < 33) {
//                    wrong++;
//                    for (String value : values) {
//                        System.out.print(value + " ");
//                    }
//                }
//                List<String> nei = Arrays.asList("Brooklyn", "Manhattan", "Bronx", "Staten Island", "Queens");
//                if (!nei.contains(values[29]))
//                    System.out.println(values[29]);
//
//
//                List<String> room = Arrays.asList("Hotel room", "Private room", "Entire home/apt", "Shared room");
//                if (!room.contains(values[33]))
//                    System.out.println(values[33]);
//
////                System.out.println(); // Print a new line after each record
//            }
//        }

//        try (FileReader fileReader = new FileReader(path);
//             FileWriter fileWriter = new FileWriter(pathOut);
//             CSVParser parser = new CSVParser(fileReader, CSVFormat.DEFAULT.withFirstRecordAsHeader());
//             CSVPrinter printer = new CSVPrinter(fileWriter, CSVFormat.DEFAULT.withHeader(parser.getHeaderNames().toArray(new String[0])))) {
//
//            // Iterating over the records and processing each line
//            for (CSVRecord record : parser) {
//                List<String> values = new ArrayList<>();
//                for (String value : record) {
//                    // Process data if needed
//                    values.add(value);
//                }
//                // Write the processed data to the output file
//                printer.printRecord(values);
//            }
//            printer.flush(); // Ensure all data is written to the file
//        }

        System.out.println(wrong);
//
//        String[] line = Utils.parseCSVLine(lineCSV);
//        String [] line = lineCSV.split(",");
//        System.out.println(line[0]);
//        System.out.println(line[29]);
//        System.out.println(line[33]);
//
//        for(String item : line)
//            System.out.println(item);
//
//        System.out.println(Arrays.toString(line));
    }
}
