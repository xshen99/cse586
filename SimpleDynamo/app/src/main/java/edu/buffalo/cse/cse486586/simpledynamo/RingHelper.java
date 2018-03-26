package edu.buffalo.cse.cse486586.simpledynamo;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * Created by Wayne on 4/27/17.
 */

public class RingHelper {

    ArrayList<Integer> portnums;
    ArrayList<String> hashkeys;
    ArrayList<Boolean> isworking;


    public RingHelper() {


        portnums = new ArrayList<Integer>();
        hashkeys = new ArrayList<String>();
        isworking = new ArrayList<Boolean>();


        String[] ports = {"5554", "5556", "5558", "5560", "5562"};

        for (int i = 0; i < ports.length; ++i) {

            try {
                String hashkey = genHash(ports[i]);
                int portnum = Integer.parseInt(ports[i]) * 2;

                Iterator<String> it = hashkeys.iterator();
                int count = 0;
                boolean isadded = false;

                while (it.hasNext()) {

                    String tmp = it.next();
                    if (tmp.compareTo(hashkey) > 0) {
                        portnums.add(count, portnum);
                        hashkeys.add(count, hashkey);
                        isworking.add(count, true);
                        isadded = true;
                        break;
                    }
                }
                if (!isadded) {

                    portnums.add(portnum);
                    hashkeys.add(hashkey);
                    isworking.add(true);
                }


            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }

        //for (int i : portnums) System.out.println(i);
        //for (String i : hashkeys) System.out.println(i);


    }

    public int getPort(String key) {

        try {
            String hashkey = genHash(key);
            for (int i = 0; i < hashkeys.size(); ++i) {
                if (hashkeys.get(i).compareTo(hashkey) >= 0) {
                    return portnums.get(i);
                }
            }
            return  portnums.get(0);
        } catch (Exception e) { }
        return 0;
    }

    public ArrayList<Integer> getNext3(String key) {
        try {
            ArrayList<Integer> tmp = new ArrayList<Integer>();
            int i = 0;
            int count = 0;
            String hashkey = genHash(key);
            for (; i < hashkeys.size(); ++i) {
                if (hashkeys.get(i).compareTo(hashkey) >= 0) {
                    if (isworking.get(i)) tmp.add(portnums.get(i));
                    ++count;
                    ++i;
                    break;
                }
            }

            while (count != 3) {
                if (i == 5) i = 0;
                if (isworking.get(i)) tmp.add(portnums.get(i));
                ++i;
                ++count;
            }

            return  tmp;
        } catch (Exception e) { }
        return null;
    }

    public ArrayList<Integer> getAlwaysNext3(String key) {
        try {
            ArrayList<Integer> tmp = new ArrayList<Integer>();
            int i = 0;
            String hashkey = genHash(key);
            for (; i < hashkeys.size(); ++i) {
                if (hashkeys.get(i).compareTo(hashkey) >= 0) {
                    tmp.add(portnums.get(i));
                    ++i;
                    break;
                }
            }

            while (tmp.size() < 3) {
                if (i == 5) i = 0;
                tmp.add(portnums.get(i));
                ++i;
            }

            return  tmp;
        } catch (Exception e) { }
        return null;
    }

    public int getSuccessor(int portnum) {
        for (int i = 0; i < 4; ++i) {
            if (portnums.get(i) == portnum) return portnums.get(i + 1);
        }
        return portnums.get(0);
    }

    public int getPredecessor(int portnum) {
        for (int i = 4; i > 0; --i) {
            if (portnums.get(i) == portnum) return portnums.get(i-1);
        }
        return portnums.get(4);
    }

    public Boolean isWorking(int port) {
        int count = 0;
        for (int portnum : portnums) {
            if (portnum == port) break;
            ++count;
        }
        return isworking.get(count);
    }

    public void SetCorruptPort(int port) {
        int count = 0;
        for (int portnum : portnums) {
            if (portnum == port) break;
            ++count;
        }
        isworking.set(count, false);
    }

    public void SetRecovery(int port) {
        int count = 0;
        for (int portnum : portnums) {
            if (portnum == port) break;
            ++count;
        }
        isworking.set(count, true);
    }



    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

}