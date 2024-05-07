//package org.hadoop;

import java.util.ArrayList;
import java.util.List;

public class Utils {
    private static final String CSV_REGEX = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";

    public static String[] parseCSVLine(String line) {
        List<String> values = new ArrayList<>();

        // Splitting the line based on the regex
        String[] splitData = line.split(CSV_REGEX);

        // Handling double quotes at the start and end of each element and replacing double double-quotes
        for (String data : splitData) {
            if (data.startsWith("\"") && data.endsWith("\"")) {
                data = data.substring(1, data.length() - 1);
                data = data.replace("\"\"", "\"");
            }
            values.add(data);
        }

        // Converting List<String> to String[]
        return values.toArray(new String[0]);
    }

}
