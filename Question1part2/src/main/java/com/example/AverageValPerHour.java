package com.example;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.example.parquetGetAverage.parquetFileOfAverages;


public class AverageValPerHour {

   
    
    /*Question 1.2: findind avarage value per hour from a given file, and writing it to a csv file.
    returns: the path to the output file.
    filePath: the path to the original file, can be a CSV or Parquet file.
    toSplitFile: true = splitting the file to small files by the date, false = creating a temp file = the original file without the incorrect lines.
    fileType: the type of the file, can be "csv" or "parquet".
    */
    public static String getAveragesOutputFile(String filePath, boolean toSplitFile, String fileType){
        SimpleEntry<List<Map<LocalDateTime, NodeAverage>> , DateTimeFormatter> averages = null;
        switch (fileType) {
            case "csv":
                averages = fileOfAveragesCSV(filePath, toSplitFile);
                break;
            case "parquet":
                averages = parquetFileOfAverages(filePath, toSplitFile);
                break;
            default:
                System.out.println("Error: the file type is not supported.");
                return null;
        }
        //for question 1.2.2.2: combining the average values from all the split files:
        Map<LocalDateTime, NodeAverage> combinedAveragePerHour = combineMaps(averages.getKey());
        //Question 1.2.2: writing the output file with the average values per hour.
        String outputFilePath = writeOutputFile(combinedAveragePerHour, filePath, averages.getValue());
        //returning the result:
        return outputFilePath;
    }

    /*Question 1.2.2: the methode checks  for incorrect or double data-lines, and calculate the avrage value for each round hour from a CSV file
    returns: a map of the average values per hour and the format of the timestamp in the file.
    filePath: the path to the original CSV-file.
    toSplitFile: true = splitting the file to small files by the date, false = creating a temp file = the original file without the incorrect lines.
    */
    public static SimpleEntry<List<Map<LocalDateTime, NodeAverage>> , DateTimeFormatter> fileOfAveragesCSV (String filePath, boolean toSplitFile) {
        String[] header = new String[2];//the header of the file.
        header[0] = "Start Time";
        header[1] = "Value";
        //finding the format of the timestamp in the file.
        DateTimeFormatter format = getFormat(filePath);//getting the format of the timestamp in the file.
        if(format == null){
            System.out.println("Error: the file is empty or the format is incorrect.");
            return null;
        }
        //Question 1.2.1: checking and cleaning the file from incorrect or double data-lines.
        Map<LocalDate, List<String>> dataByDate = checkAndSplit(filePath, format);
        
    
        //Question 1.2.2: for 1.2.2.1 :creating a file without the incorrect lines.
        //                for 1.2.2.2 :splitting the file to small files by the date.
        List<String> filePaths = createSplitFiles(dataByDate, filePath, header, toSplitFile);
        //Question 1.2.2: calculating the average value per hour.
        List<Map<LocalDateTime, NodeAverage>> averagePerHourMaps = new ArrayList<>();
        for(String filePathByDate : filePaths){
            Map<LocalDateTime, NodeAverage> averagePerHour = calculateAveragePerHour(filePathByDate, format);
            averagePerHourMaps.add(averagePerHour);
        }
        //deleting the split files:
        for(String filePathByDate : filePaths){
            try {
                java.nio.file.Files.delete(java.nio.file.Paths.get(filePathByDate));
            } catch (IOException e) {
                System.out.println("Error deleting the file: " + filePathByDate + " - " + e.getMessage());
            }
        }

        SimpleEntry<List<Map<LocalDateTime, NodeAverage>>, DateTimeFormatter> result = new SimpleEntry<>(averagePerHourMaps, format);
        //returning the result:
        return result;

    }

    /*Question 1.2.1: finding the format of the timestamp in the file.
     * returns: the format of the timestamp in the file.
     * filePath: the path to the original file.
     */
    //note: the methode will consider the entire file is the same format.
    private static DateTimeFormatter getFormat(String filePath){
     
    DateTimeFormatter timestampsformat1 = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");
    DateTimeFormatter timestampsformat2 = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss");
    DateTimeFormatter timestampsformat3 = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm");
    DateTimeFormatter timestampsformat4 = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm");
    try(BufferedReader reader = new BufferedReader(new FileReader(filePath))){
        reader.readLine();//skip the header line.
        String nextLine;
        while((nextLine = reader.readLine()) != null){
            String[] lineVal = nextLine.split(",");
            if(lineVal.length ==2){// check that the line contanes onli a timstemp and a value, not more and not less.
                String timestemp = lineVal[0].trim();//getting the timestamp from the line.
                //finding the format of the timestamp.
                //note: the methode will consider the entire file is the same format.
                try {
                    LocalDateTime.parse(timestemp, timestampsformat1);
                    return timestampsformat1;
                } catch (Exception e) {
                    try {
                         LocalDateTime.parse(timestemp, timestampsformat2);
                        return timestampsformat2;
                    } catch (Exception e1) {
                        try {
                            LocalDateTime.parse(timestemp, timestampsformat3);
                            return timestampsformat3;
                        } catch (Exception e2) {
                            try {
                                LocalDateTime.parse(timestemp, timestampsformat4);
                                return timestampsformat4;
                            } catch (Exception e3) {
                            }
                        }
                    }
                }
            }
        }
    }catch (IOException e){
        System.out.println("Error reading the file: " + e.getMessage());
    }
    return null;
}

    /*Question 1.2.1: checking the file for incorrect or double data-lines.
     * returns: a map of the lines split by date.
     *          NOTE: the methode sorts the lines by date, for question 1.2.2.2: splitting the file to small files by the date.
     *filePath: the path to the original file.
     *header: the header of the file, to be used for the split files.  
     *format: the format of the timestamp in the file. 
     */
    private static Map<LocalDate, List<String>> checkAndSplit(String filePath, DateTimeFormatter format){
        Map<LocalDate, List<String>> splitByDate = new HashMap<>();
        Set<LocalDateTime> uniqueTimestamps = new HashSet<>();
        LocalDateTime timestampDate = null;

        try(BufferedReader reader = new BufferedReader(new FileReader(filePath))){
            String nextLine;
            boolean isFirstLine = true;
            
            while((nextLine = reader.readLine()) != null){
                String[] lineVal = nextLine.split(",");

                if(isFirstLine){//skip the header line:
                    isFirstLine = false;
                    continue;//to the first line of data.
                }

                if(lineVal.length ==2){// check that the line contanes onli a timstemp and a value, not more and not less.
                    String timestemp = lineVal[0].trim();//getting the timestamp from the line.
                    String value = lineVal[1].trim();//getting the value from the line.
                    
                    try { //check if the format is correct.
                        timestampDate = LocalDateTime.parse(timestemp, format);
                         

                        try {//check if the value is a decimal number.
                            double val =Double.parseDouble(value);
                            if(!Double.isNaN(val)){//checks for a nan value.
                            //checking for double timestamps:
                                if(!uniqueTimestamps.contains(timestampDate)){
                                    uniqueTimestamps.add(timestampDate);
                                    LocalDate date = timestampDate.toLocalDate();//getting the date from the timestamp for the split-file name.
                                    if(!splitByDate.containsKey(date)) {
                                        splitByDate.put(date, new ArrayList<>());
                                    }
                                    splitByDate.get(date).add(nextLine);//adding the line to a list of lines for the date.
                            
                              }
                                else{
                               // System.out.println("Timestamp is not unique: " + nextLine);
                                }
                            }else{
                                System.out.println("Value is not a decimal number(NaN): " + nextLine);
                            }
                        } catch (Exception e) {
                            System.out.println("Value is incorrect: " + nextLine);
                        }
                        
                    } catch (Exception e) {
                        System.out.println("Timestamp is incorrect: " + nextLine);
                    }

                }else{
                    System.out.println("Line is incorrect: " + nextLine);
                }
            }
            
        }catch (IOException e){
            System.out.println("Error reading the file: " + e.getMessage());
        }
        return splitByDate;
    }

    /*Question 1.2.2: creating the split files by the date.
     * returns: a list of the file paths of the split files.
     * filePath: the path to the original file.
     * header: the header of the file, to be used for the split files.
     * toSplitFile: true = splitting the file to small files by the date, false = creating a temp file = the original file without the incorrect lines.
     */
    private static List<String> createSplitFiles(Map<LocalDate, List<String>> dataByDate, String filePath, String[] header, boolean toSplitFile){
        List<String> filePaths = new ArrayList<>();
        DateTimeFormatter fileNameByDate = DateTimeFormatter.ofPattern("yyyyMMdd");
        //for question 1.2.2.1: working with 1 file - creating a temp file = the original file without the incorrect lines:
        if(!toSplitFile){
            String tempFilePath = filePath.replace(".csv", "_temp.csv");
            try(BufferedWriter writer = new BufferedWriter(new FileWriter(tempFilePath))){
                writer.write(String.join(",", header) + "\n");//writing the header to the temp file.
                for(List<String> lines : dataByDate.values()){
                    for(String line : lines){
                        writer.write(line + "\n");//writing the lines to the temp file.
                    }
                }
                filePaths.add(tempFilePath);//adding the temp file path to the list of file paths.
                writer.close();
            }catch (IOException e){
                System.out.println("Error writing the temp file: " + e.getMessage());
            }
            return filePaths;    
        }

        //for question 1.2.2.2: splitting the file to small files by the date:
        for(Map.Entry<LocalDate, List<String>> entry : dataByDate.entrySet()){
            LocalDate date = entry.getKey();
            List<String> lines = entry.getValue();
            String fileName = date.format(fileNameByDate) + ".csv";
            String filePathByDate = filePath.replace(".csv", "_" + fileName);
            try(BufferedWriter writer = new BufferedWriter(new FileWriter(filePathByDate))){
                writer.write(String.join(",", header) + "\n");//writing the header to the split file.
                for(String line : lines){
                    writer.write(line + "\n");//writing the lines to the split file.
                }
                filePaths.add(filePathByDate);//adding the split file path to the list of file paths.
                writer.close();
            }catch (IOException e){
                System.out.println("Error writing the split file: " + e.getMessage());
            }
        }
        return filePaths;

    }


    /*Question 1.2.2: calculating the average value per hour.
     * returns: a map of the average values per hour.
     * filePath: the path to the file.
     * timestampFormatter: the format of the timestamp in the file.
     */
    private static Map<LocalDateTime, NodeAverage> calculateAveragePerHour(String filePath, DateTimeFormatter timestampFormatter){
        Map<LocalDateTime, NodeAverage> avergePerHour = new HashMap<>();
        
        try(BufferedReader reader = new BufferedReader(new FileReader(filePath))){
            String nextLine;
            boolean isFirstLine = true;
            while((nextLine = reader.readLine())!= null){
                //skip the header line:
                if(isFirstLine){
                    isFirstLine = false;
                    continue;//to the first line of data.
                }
                String[] lineVal = nextLine.split(",");
                try {
                    LocalDateTime timestamp = LocalDateTime.parse(lineVal[0].trim(), timestampFormatter);
                    double value = Double.parseDouble(lineVal[1].trim());
                    LocalDateTime hour = timestamp.withMinute(0).withSecond(0).withNano(0);//getting the hour from the timestamp.
                    if(!avergePerHour.containsKey(hour)){
                        avergePerHour.put(hour, new NodeAverage(value));//creating a new node for the hour.
                    }else{
                        avergePerHour.get(hour).setAverage(value);//updating the node for the hour.
                    }
                } catch (Exception e) {
                    System.out.println("Error parsing the line: " + nextLine + " - " + e.getMessage());
                }
            }
        }catch (IOException e){
            System.out.println("Error reading the file: " + e.getMessage());
        }
        return avergePerHour;
    }

    /*combining the average values from all the split files.
     * returns: a map of the average values per hour.
     * averagePerHourMaps: a list of maps of the average values per hour.
     */
    private static Map<LocalDateTime, NodeAverage> combineMaps(List<Map<LocalDateTime, NodeAverage>> averagePerHourMaps ){
        Map<LocalDateTime, NodeAverage> combinedMap = new HashMap<>();
        for(Map<LocalDateTime, NodeAverage> map : averagePerHourMaps){
            for(Map.Entry<LocalDateTime, NodeAverage> entry : map.entrySet()){
                LocalDateTime hour = entry.getKey();
                NodeAverage nodeAverage = entry.getValue();
                if(!combinedMap.containsKey(hour)){
                    combinedMap.put(hour, nodeAverage);//creating a new node for the hour.
                }else{
                    combinedMap.get(hour).setAverage(nodeAverage);//updating the node for the hour.
                }
            }
        }
        return combinedMap;
    }

    /*Question 1.2.2: writing the output file with the average values per hour.
     * returns: the path to the output file.
     * averagePerHour: the map of the average values per hour.
     * filePath: the path to the original file.
     * outputFormatter: the format of the timestamp in the file.
     * NOTE: the output file will be in the same format as the original file, but with the average values per hour.
     */
    private static String writeOutputFile (Map<LocalDateTime, NodeAverage> averagePerHour, String filePath, DateTimeFormatter outputFormatter){
       
        String outputFilePath = filePath.replace(".csv", "_averages.csv");
        try(BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath))){
            writer.write("Start Time,Average Value\n");//writing the header to the output file.
            for(Map.Entry<LocalDateTime, NodeAverage> entry : averagePerHour.entrySet()){
                LocalDateTime hour = entry.getKey();
                double averageValue = entry.getValue().getValue();
                writer.write(hour.format(outputFormatter) + "," + averageValue + "\n");//writing the lines to the output file.
            }
            writer.close();
        }catch (IOException e){
            System.out.println("Error writing the output file: " + e.getMessage());
        }
        return outputFilePath;
    }


    
}