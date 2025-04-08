package com.example;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Path;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;

public class Main {
    public static void main(String[] args) {
        try {
            URL resource = Main.class.getClassLoader().getResource("time_series.csv");
            if (resource == null) {
                System.err.println("Resource not found: time_series.csv");
                return;
            }

            URI resourceUri = resource.toURI();
            Path path = Paths.get(resourceUri);
            String filePath = path.toString();

            boolean toSplitFile = true;
            String fileType = "csv";

            String outputFilePath = AverageValPerHour.getAveragesOutputFile(filePath, toSplitFile, fileType);
            System.out.println("Output file path: " + outputFilePath);

        } catch (Exception e) {
            System.err.println("Error occurred while processing the file: ");
            e.printStackTrace();
        }
       
    }
}
