/*QUESTION 1.2.3: Advantages of parquet file format:
 * Parquet files are binary,and suport compression, which makes them smaller than CSV files, which are text files.
 * therefor working with parquet files significantly reduces the size of the file.
 * moreover, parquet files are columnar, which means that they are optimized for reading and writing data in columns rather than rows.
 * another advantage of parquet files is that it preserves data types such as timestamps.*/

package com.example;
import java.io.IOException;

import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.avro.generic.GenericRecord;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.AbstractMap.SimpleEntry;

import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

public class parquetGetAverage {

    /*Question 1.2.4: the methode checks  for incorrect or double data-lines, and calculate the avrage value for each round hour from a parquet file
    returns: a map of the average values per hour and the format of the timestamp in the file.
    filePath: the path to the original parquet-file.
    toSplitFile: true = splitting the file to small files by the date, false = creating a temp file = the original file without the incorrect lines.
    */
    public static SimpleEntry<List<Map<LocalDateTime, NodeAverage>> , DateTimeFormatter> parquetFileOfAverages (String filePath, boolean toSplitFile) {
        //Question 1.2.1: getting the schema of the parquet file.
        Schema schema = getSchema(filePath);
        //Question 1.2.3: finding the format of the timestamp in the file.
        DateTimeFormatter format = getFormat(filePath);
        if(format == null){
            System.out.println("Error: could not find the format of the timestamp in the file.");
            return null;
        }
        //Question 1.2.3: checking the file for incorrect or double data-lines.
        Map<LocalDate, List<GenericRecord>> recordsByDate = checkAndSplitPar(filePath, format);
        //Question 1.2.4: creating a new parquet split files with the correct data.
        List<String> filePaths = createSplitFilesPar(recordsByDate, filePath, toSplitFile, schema);
        //Question 1.2.4: calculating the average value per hour.
        List<Map<LocalDateTime, NodeAverage>> averagePerHourMaps = new ArrayList<>();
        for(String filePathByDate : filePaths){
            Map<LocalDateTime, NodeAverage> averagePerHour = calculateAveragePerHourParquat(filePathByDate, format);
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

    /*Question 1.2.2: getting the schema of the parquet file.
     * returns: the schema of the parquet file.
     * filePath: the path to the original file.
     */
    private static Schema getSchema(String filePath){
        try {
            Path path = new Path(filePath);
            Configuration config = new Configuration();
            ParquetMetadata metadata = ParquetFileReader.readFooter(config, path);
            String schemaString = metadata.getFileMetaData().getKeyValueMetaData().get("parquet.avro.schema");
            if (schemaString == null) {
                System.err.println("Schema not found in Parquet file metadata.");
                return null;
            }
            return new Schema.Parser().parse(schemaString);
        } catch (Exception e) {
            System.err.println("Error reading Parquet file: " + e.getMessage());
            return null;
        }
    }
    

    /*Question 1.2.3: finding the format of the timestamp in the file.
     * returns: the format of the timestamp in the file.
     * filePath: the path to the original file.
     */
    //note: the methode will consider the entire file is the same format.
    private static final DateTimeFormatter getFormat(String filePath){
        DateTimeFormatter timestampsformat1 = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");
        DateTimeFormatter timestampsformat2 = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss");
        DateTimeFormatter timestampsformat3 = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm");
        DateTimeFormatter timestampsformat4 = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm");
        
        try(ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(new Path(filePath)).build()) {
            GenericRecord record = reader.read();
            String timestamp = record.get("timestamp").toString();
            //finding the format of the timestamp.
            //note: the methode will consider the entire file is the same format.
            try {
                LocalDateTime.parse(timestamp, timestampsformat1);
                return timestampsformat1;
            } catch (Exception e) {
            }
            try {
                LocalDateTime.parse(timestamp, timestampsformat2);
                return timestampsformat2;
            } catch (Exception e) {
            }
            try {
                LocalDateTime.parse(timestamp, timestampsformat3);
                return timestampsformat3;
            } catch (Exception e) {
            }
            try {
                LocalDateTime.parse(timestamp, timestampsformat4);
                return timestampsformat4;
            } catch (Exception e) {
            }
            
        } catch (Exception e) {
            System.out.println("Error reading Parquet file: " + e.getMessage());
        }
        return null;
    }


    /*Question 1.2.3: checking the file for incorrect or double data-lines.
     * returns: a map of the records split by date.
     *          NOTE: the methode sorts the records by date, for question 1.2.3.2: splitting the file to small files by the date.
     * filePath: the path to the parquet file.
     * format: the format of the timestamp in the parquet file.
   */
    private  static Map<LocalDate, List<GenericRecord>> checkAndSplitPar(String filePath, DateTimeFormatter format){
        Map<LocalDate, List<GenericRecord>> recordsByDate = new HashMap<>();
        Set<LocalDateTime> uniqueTimestamps = new HashSet<>();
        LocalDateTime timestampDate = null;

        try {
            ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(new Path(filePath)).build();
            GenericRecord record;
            while((record = reader.read()) !=null){
                String timestamp = record.get("timestamp").toString();
                Object value = record.get("value");

                try {//check if the format is correct.
                    timestampDate = LocalDateTime.parse(timestamp, format);

                    if(value instanceof Number) {//check if the value is a decimal number.
                        double val = ((Number) value).doubleValue();
                        if(!Double.isNaN(val)){//checks for a nan value.
                        //checking for double timestamps:
                            if(!uniqueTimestamps.contains(timestampDate)){
                                uniqueTimestamps.add(timestampDate);
                                LocalDate date = timestampDate.toLocalDate();//getting the date from the timestamp for the split-file name.
                                if(!recordsByDate.containsKey(date)) {
                                    recordsByDate.put(date, new ArrayList<>());
                                }
                                recordsByDate.get(date).add(record);//adding the line to a list of lines for the date.
                            }
                            else{
                                System.out.println("Timestamp is not unique: " + record.toString());
                            }
                        }else{
                            System.out.println("Value is not a decimal number(NaN): " + record.toString());
                        }
                    } else {
                        System.out.println("Value is incorrect: " + record.toString());
                    }
                } catch (Exception e) {
                    System.out.println("Timestamp is incorrect: " + record.toString());
                }
            }
            reader.close();
        } catch (Exception e) {
            System.out.println("Error reading Parquet file: " + e.getMessage());
        }
        
        return recordsByDate;

    }

    /*Question 1.2.4: creating a new parquet split files with the correct data.
     * returns: a list of the paths to the new files.
     * filePath: the path to the parquet file.
     * toSplitFile: true if the file should be split by date, false if it should be one file.
     * schema: the schema of the parquet file.
     */
    private static List<String> createSplitFilesPar(Map<LocalDate, List<GenericRecord>> recordsByDate, String filePath, boolean toSplitFile, Schema schema){
         List<String> filePaths = new ArrayList<>();
        DateTimeFormatter fileNameByDate = DateTimeFormatter.ofPattern("yyyyMMdd");
        //for question 1.2.4.1: working with 1 file - creating a temp file = the original file without the incorrect lines:
        if(!toSplitFile){ 
            String tempFilePath = filePath.replace(".parquet", "_temp.parquet");
            try(ParquetWriter<GenericRecord> writer =AvroParquetWriter.<GenericRecord>builder(new Path(tempFilePath)).withSchema(schema).withCompressionCodec(CompressionCodecName.SNAPPY).build()){
                for (List<GenericRecord> records : recordsByDate.values()) {

                    for (GenericRecord record : records) {
                        writer.write(record);
                    }
                }
                filePaths.add(tempFilePath);
                writer.close();
            } catch (Exception e) {
                System.out.println("Error writing Parquet file: " + e.getMessage());
            }
            return filePaths; 
        }

         //for question 1.2.2.2: splitting the file to small files by the date:
         for(Map.Entry<LocalDate, List<GenericRecord>> entry : recordsByDate.entrySet()) {
            LocalDate date = entry.getKey();
            List<GenericRecord> records = entry.getValue();
            String splitFilePath = filePath.replace(".parquet", "_" + date.format(fileNameByDate) + ".parquet");
            try(ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new Path(splitFilePath)).withSchema(schema).withCompressionCodec(CompressionCodecName.SNAPPY).build()){
                for (GenericRecord record : records) {
                    writer.write(record);
                }
                filePaths.add(splitFilePath);
                writer.close();
            } catch (Exception e) {
                System.out.println("Error writing Parquet file: " + e.getMessage());
            }
        } 
        return filePaths;
    }

    /*Question 1.2.4: calculating the average value per hour.
        * returns: a map of the average value per hour.
        * filePath: the path to the parquet file.
        * format: the format of the timestamp in the parquet file.
     */
    private static Map<LocalDateTime, NodeAverage> calculateAveragePerHourParquat(String filePath, DateTimeFormatter format){
        Map<LocalDateTime, NodeAverage> avergePerHour = new HashMap<>();
        
        try(ParquetReader<GenericRecord> reader =AvroParquetReader.<GenericRecord>builder(new Path(filePath)).build()){
            GenericRecord record;

            while((record = reader.read()) != null){
                String timestamp = record.get("timestamp").toString();
                Object value = record.get("value");
                try {
                    LocalDateTime timestampDate = LocalDateTime.parse(timestamp, format);
                    double val = ((Number) value).doubleValue();
                    LocalDateTime hour = timestampDate.withMinute(0).withSecond(0).withNano(0);//getting the hour from the timestamp.
                    if(!avergePerHour.containsKey(hour)){
                        avergePerHour.put(hour, new NodeAverage(val));//creating a new node for the hour.
                    }else{
                        avergePerHour.get(hour).setAverage(val);//updating the node for the hour.
                    }

                } catch (Exception e) {
                    System.out.println("Error parsing the line: " + record.toString());
                }
            }
        } catch (IOException ex) {
            System.out.println("Error reading Parquet file: " + ex.getMessage());
        }
        return avergePerHour;
    }
}
