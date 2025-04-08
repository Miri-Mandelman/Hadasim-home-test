/*This class represent an object NodeError, to contain the Error Code and its frequency. */

public class NodeError {
    private String errorCode;
    private int frequency;

    public NodeError(String code, int freq){
        errorCode = code;
        frequency = freq;
    }

    public String getErrorCode(){
        return errorCode;
    }

    public int getFrequency(){
        return frequency;
    }

    public void frequencyPlusOne(){
        frequency++;
    }

    public void addFrequency(int temp){
        frequency+=temp;
    }

    public boolean sameError(String code){
        return errorCode.equals(code);
    }
    
    public boolean sameError(NodeError other){
        return errorCode.equals(other.getErrorCode());
    }

    public boolean isBigger(NodeError other){
        return(this.frequency>=other.frequency);
    }

    public void print(){
        System.out.println(errorCode+" : "+frequency);
    }
    
}
