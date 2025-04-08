package com.example;
public class NodeAverage {
    private double value;
    private int count;

    public NodeAverage(double val){
        this.value = val;
        this.count = 1;
    }

    public double getValue() {
        return value;
    }

    public void setAverage(double val) {
        value = (value*count + val) / (count+1);
        count++;
    }

    public void setAverage(NodeAverage other){
        this.value = (this.value*this.count +other.value) / (this.count+other.count);
        this.count += other.count;
    }
    
}
