package edu.buffalo.cse.cse486586.simpledht;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Created by Wayne on 4/9/17.
 */


public class Message implements Serializable {

    public char type;
    //J-join, U-update
    public String myport;
    public String pre;
    public String next;
    public String key;
    public String val;
    public HashMap<String, String> ret;


    public Message(char type, String myport, String pre, String next, String key, String val, HashMap<String, String> hm) {
        this.type = type;
        this.myport = myport;
        this.pre = pre;
        this.next = next;
        this.key = key;
        this.val = val;
        this.ret = hm;
    }
}