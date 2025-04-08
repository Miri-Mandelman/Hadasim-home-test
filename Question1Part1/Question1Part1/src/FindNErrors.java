
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class FindNErrors{

    /*Question 1.1: from a given large file of logs, finding the N most frequent error-codess in the logs.
     * returns: the list of the n most frequent error codes.
     * originalFilePath: the source path of the original file.
     * n: the number of most frequent error codes to find.

    Question 1.1.5:
      Time complexity: O(klogk) - as detailed below.
                            the methode calls for 4 other methods, and their time complexity is:
                            1. splitToFiles: O(k) - when k is the number of lines in the given logs-file.
                            2. freqAllFiles: O(k) - when k is the number of lines in the given logs-file.
                            3. attachLists: O(k) - when k is the number of lines in the given logs-file.
                            4. getN: O(klogk) - when k is the number of lines in the given logs-file.
                            altogether, the time complexity is O(klogk).

      Space complexity: O(m) -as detailed below:
                            the methode calls for 4 other methods, and their time complexity is:
                            1. splitToFiles: O(m) - when m is the size of the given logs-file.
                            2. freqAllFiles: O(k) - when k is the number of lines in the given logs-file.
                            3. attachLists: O(k) - when k is the number of lines in the given logs-file.
                            4. getN: O(n) - when n is the number of most frequent error codes to find.
                            altogether, the space complexity is O(m+k+n) = O(m).
            !! Note !! : the space comlaxity can be inprouved by not spliting the file to small files in addvence,
                        and instead, using only one small file, writing the wanted x lines to it, analizing it, deliting and writing the next x lines.
                        however, I chose not to do so, in order to answer the request of parts 1.1.1,1.1.2,1.1.3.
                        which by my understanding demands to split the file in advance, before analyzing it.
                         
     */
    public static List<NodeError> nMostFrequentErrors(String originalFilePath, int n ){
        final int numOfLines = 1000;
        int numOfFiles;
        try {
            //Q 1.1.1: split the file to small files:
           numOfFiles =splitToFiles(originalFilePath, numOfLines);
            //Q 1.1.2: find the frequency of each error code in each file:
            LinkedList<LinkedList<NodeError>> errorsListslist= freqAllFiles(numOfFiles, originalFilePath);
            //delete the small files that were created:
            for (int i = 1; i <= numOfFiles; i++) {
                String tempPath = originalFilePath + "_part_" + i + ".txt";
                java.nio.file.Files.delete(java.nio.file.Paths.get(tempPath));
            }

            //Q 1.1.3: attach the lists of errors from all files to one list:
            List<NodeError> errorAllList = attachLists(errorsListslist);
            //Q 1.1.4: find and return the n most frequent error codes
            return getN(errorAllList, n);
         } catch (IOException e) {
            System.err.println("Error in reading the file: " + originalFilePath);
            e.printStackTrace();
            return null;
        }
    }


    /*Question 1.1.1:
      splitToFiles is a function for splitting the given file to small files,
         each file will contane number of lines as given.
      filePath: the source path of the file to split.
      numOfLines: the number of lines each small file will contane.

                         For Question 1.1.5:
      Time complexity: O(k) - when k is the number of lines in the given logs-file.
                            the methode reed the file line by line and writing it to small files.
      Space complexity: O(m) - when m is the size of the given logs-file.
                                the methode creates new file for each given number of lines from the given file,
                                together it stores all the lines from the origin file, therefor equals to the file size.
    */
    private static int splitToFiles(String filePath, int numOfLines) throws IOException{
        int fileCount =1;
        //Open the given file for reading:
        try(BufferedReader reader = new BufferedReader(new FileReader(filePath))){
            
            int lineCount =0;
            String nextLine;
            BufferedWriter writer = null;

            //reading the file line by line:
            while((nextLine = reader.readLine())!=null){
                //handling case of a full file by closing the file and opening new one:
                if(lineCount % numOfLines == 0){
                    if(writer != null){//close the full file.
                        writer.close();
                    }
                    String newPath = filePath + "_part_"+ fileCount+".txt";
                    writer = new BufferedWriter(new FileWriter(newPath));
                    fileCount++;
                    lineCount = 0; //starting to count lines for the new file.
                }
                //writing the current line from the given file to the smal file:
                writer.write(nextLine);
                writer.newLine();
                lineCount++;
            }

            if(writer != null) writer.close();//closing the last file

        }
        fileCount--;//decreasing the file count by 1, because the last file was not counted in the loop.
        return fileCount;
    }


    /*Question 1.1.2
     freqAllFiles finds for each file the frequency of each error-code in it.
     return: a list of lists of nodes that contanes an error-code and its frequency,
            each list is from one file.
     numOfFiles: number of files that need to find the errors from.
     filePath: the source path of the original file.

                         For Question 1.1.5:
      Time complexity: O(k) - when k is the number of lines in the given logs-file.
                            the methode reads and analyzes each line from each file that was created in the previous step,
                            alltogether it equals to the number of lines in the given logs-file.
      Space complexity: O(k) - in the wrost case, and O(n) - when n is the given n number, in the best case.
                              for each error-code in each line in each file, the methode checks if its new, if so createsand contanes a nodeError.
                              in the wrost case, all the lines are different and the space complexity is O(k).
                              in the best case, there are only n different errors-code (or less) and the space complexity is O(n).                       
     */
    private  static LinkedList<LinkedList<NodeError>> freqAllFiles(int numOfFiles,String filePath){
        LinkedList<LinkedList<NodeError>> freqLists = new LinkedList<>();
        LinkedList<NodeError> tempList = new LinkedList<>();
        String tempPath; 
        for (int i = 1; i <= numOfFiles; i++) {
            tempPath = filePath+ "_part_"+i+".txt";
            try {
                tempList = freqPerFile(tempPath);// call for a private methode to work on one file.
            } catch (IOException e) {
                System.err.println("problem with file numer: "+i);
                e.printStackTrace();
            }
            freqLists.add(tempList);
        }
        return freqLists;
    }

    /*Helper methode for finding the frequency of each error-code in a given file.
     returns: a list of nodes that contanes an error-code and its frequency on the given file.
     tempPath: the source path of the given file.
    */
    private static LinkedList<NodeError> freqPerFile(String tempPath)throws IOException {
        LinkedList<NodeError> tempList = new LinkedList<>();
        //defining the pattern to find the error-code in the line:
        Pattern errPattern = Pattern.compile("Error: (\\w+)");
        String nextLine;

        try(BufferedReader reader = new BufferedReader(new FileReader(tempPath))){
            while((nextLine=reader.readLine()) != null){
                Matcher matcher = errPattern.matcher(nextLine);
                if(matcher.find()){
                    String tempError = matcher.group(1);//saving the error code in this line.
                    boolean existsInList=false;
                    int i ;
                    for( i=0; i < tempList.size(); i++){//finding if the error codewas allready in this file
                        if(tempList.get(i).sameError(tempError)){
                            existsInList =true;
                            break;
                        }
                    }
                    if(existsInList){//if this code was allready in this file:
                        tempList.get(i).frequencyPlusOne();//Increases the frequency by 1.
                    }
                    else{//if it's a new code in this file:
                        NodeError err = new NodeError(tempError, 1);
                        tempList.add(err);
                    }
                    
                }
            }
        }

        return tempList;
    }



    /*Question 1.1.3:
     * attachLists attaches the lists of errors from all files to one list.
     * return: a list of nodes that contanes an error-code and its frequency in all files.
     * freqLists: a list of lists of nodes to be attached.

                         For Question 1.1.5:
      Time complexity: O(k) - when k is the number of lines in the given logs-file.
                            the methode combines all lists to one by using a hash map,
                            in hashMap the time complexity is O(1) for the 'containsKey' ,'get' and 'put' methods.                     
                            and the loop goes over all the nodes in the lists, which are k nodes in the wrost case.
                            therefor the time complexity is O(k).
      Space complexity: O(k) - the methode create and returns a sorted list of all the nodes.
                            the size of the list is at most k, when all the error codes are different.
                            in the best case, when there are only n different errors-code (or less) and the space complexity is O(n).
     */
    private  static List<NodeError> attachLists(LinkedList<LinkedList<NodeError>> freqLists){
       Map<String, NodeError> allFreq = new HashMap<>(); // to save the error code and its frequency in all files.
        for (LinkedList<NodeError> freqList : freqLists) {
            for (NodeError node : freqList) {
                String errorCode = node.getErrorCode();
                if (allFreq.containsKey(errorCode)) {
                    allFreq.get(errorCode).addFrequency(node.getFrequency());
                } else {
                    allFreq.put(errorCode, node);
                }
            }
        }
        List<NodeError> allFreqList = new ArrayList<>(allFreq.values());
        return allFreqList;
    }


    /*Question 1.1.4:
     * getN sorts a given list of nodes by their frequency and returns the n most frequent error codes.
     * return: a list of the n most frequent error codes.
     * freqList: the list of nodes to find the n most frequent values from.
     * n: the number of most frequent error codes to find.

                         For Question 1.1.5:
      Time complexity: O(klogk) - when k is the number of lines in the given logs-file.
                            the methode sorts the list of nodes by their frequency, using the List.sort methode with a comparator, which is O(klogk).
                            and then it goes over the n most frequent nodes, which is O(n).
                            therefor the time complexity is O(klogk+n) = O(klogk).
      Space complexity: O(n) - the methode creates a new list of size n to return.
     */
    private static LinkedList<NodeError> getN(List<NodeError> freqList, int n){
        LinkedList<NodeError> nList = new LinkedList<>();
        freqList.sort(Comparator.comparingInt(NodeError::getFrequency));
        for (int i = 0; i < n && i < freqList.size(); i++) {
            nList.add(freqList.get(freqList.size()-1-i));
        }
        return nList;
    }




    public static void main(String[] args) {
        
        String filePath = "resources/logs.txt";
        int n = 5;
        List<NodeError> result = nMostFrequentErrors(filePath, n);
        if (result != null) {
            System.out.println("The " + n + " most frequent error codes are:");
            for (NodeError node : result) {
                node.print();
            }
        } else {
            System.out.println("An error occurred while processing the file.");
        }
       
    }

}