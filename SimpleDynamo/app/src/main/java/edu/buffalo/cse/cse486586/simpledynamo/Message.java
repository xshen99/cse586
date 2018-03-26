package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Created by Wayne on 4/9/17.
 */


public class Message implements Serializable {

    public char type;
    public int fromport;
    public HashMap<String, String> keyvalueset;
    public String key;
    public String value;


    public Message(char type, int fromport, String key, String value, HashMap<String, String> keyvalueset) {
        this.type = type;
        this.fromport = fromport;
        this.keyvalueset = keyvalueset;
        this.key = key;
        this.value = value;
    }
}