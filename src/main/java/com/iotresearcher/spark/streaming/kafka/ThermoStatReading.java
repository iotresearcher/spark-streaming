package com.iotresearcher.spark.streaming.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;

/**
 * Created by shazi on 11/1/2016.
 */
public class ThermoStatReading implements Serializable {

    private int temperature;

    private int humidity;

    public ThermoStatReading() {

    }

    public ThermoStatReading(int temperature, int humidity) {
        this.temperature = temperature;
        this.humidity = humidity;
    }

    public int getTemperature() {
        return temperature;
    }

    public void setTemperature(int temperature) {
        this.temperature = temperature;
    }

    public int getHumidity() {
        return humidity;
    }

    public void setHumidity(int humidity) {
        this.humidity = humidity;
    }

    public String toString() {
        return "Temperature: "+temperature+", Humidity: "+humidity;
    }
}
