package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;


public class SimpleDhtProvider extends ContentProvider {

    private HashMap<String, String> LocalHM;

    private final String TAG = "SimpleDht";
    private String myPort;
    private String NextPort;
    private String PrePort;
    private String myId;
    private String NextId;
    private String PreId;
    private boolean isConnected;

    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";

    static final String REMOTE_PORT0 = "11108";
    static final int SERVER_PORT = 10000;

    Message retm;


    @Override
    public boolean onCreate() {
        LocalHM = new HashMap<String, String>();

        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        try {
            myId = genHash(String.valueOf(Integer.parseInt(myPort) / 2));
        } catch (Exception e) { }

        isConnected = false;

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return false;
        }

        if (!myPort.equals(REMOTE_PORT0)) {
            Message m = new Message('J', myPort, null, null, null, null, null);
            Send(REMOTE_PORT0, m);
        }

        return false;
    }

    private boolean isMyNode(String theId) {
        if (myId.compareTo(NextId) < 0) {
            if (theId.compareTo(NextId) < 0 && theId.compareTo(myId) > 0) return true;
        }
        else {
            if (theId.compareTo(myId) > 0 || theId.compareTo(NextId) < 0) return true;
        }
        return false;
    }

    private boolean isMyObject(String theId) {
        if (myId.equals(theId)) return true;

        if (myId.compareTo(PreId) > 0) {
            if (theId.compareTo(myId) < 0 && theId.compareTo(PreId) > 0) return true;
        }
        else {
            if (theId.compareTo(PreId) > 0 || theId.compareTo(myId) < 0) return true;
        }
        return false;
    }


    private void Join(Message m) {
        String theId = "";
        String thePort = m.myport;
        try {
            theId = genHash(String.valueOf(Integer.parseInt(thePort)/2));
        } catch (Exception e) { }
        if (PrePort == null) {
            PrePort = thePort;
            PreId = theId;
            NextPort = thePort;
            NextId = theId;
            Message newm = new Message('U', null, myPort, myPort, null, null, null);
            Send(thePort, newm);
            isConnected = true;
        }
        else if (isMyNode(theId)) {
            Message newm1 = new Message('U', null, myPort, NextPort, null, null, null);
            Send(thePort, newm1);
            Message newm2 = new Message('U', null, thePort, null, null, null, null);
            Send(NextPort, newm2);
            NextPort = thePort;
            NextId = theId;
        }
        else {
            Send(NextPort, m);
        }
    }

    private void UpdateN(Message m) {
        if (m.pre != null) {
            PrePort = m.pre;
            try {
                PreId = genHash(String.valueOf(Integer.parseInt(PrePort)/2));
            } catch(Exception e) { }
        }
        if (m.next != null) {
            NextPort = m.next;
            try {
                NextId = genHash(String.valueOf(Integer.parseInt(NextPort)/2));
            } catch(Exception e) { }

            Iterator it = LocalHM.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry)it.next();
                doDelete((String)pair.getKey());
                doInsert((String)pair.getKey(), (String)pair.getValue());
            }
        }
        if(!isConnected) isConnected = true;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        String key = (String) values.get(KEY_FIELD);
        String val = (String) values.get(VALUE_FIELD);
        doInsert(key, val);
        return null;
    }

    private void doInsert(String key, String val) {
        if (!isConnected) {
            LocalHM.put(key, val);
            return;
        }

        String theId = "";
        try {
            theId = genHash(key);
        } catch (Exception e) { }
        if (isMyObject(theId)) {
            LocalHM.put(key, val);
        }
        else {
            Message m = new Message('I', null, null, null, key, val, null);
            Send(NextPort, m);
        }
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        return doQuery(selection, myPort);
    }

    private Cursor doQuery(String selection, String port) {
        if (selection.equals("@") || (!isConnected && selection.equals("*"))) {
            MatrixCursor result = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});
            Iterator it = LocalHM.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry)it.next();
                result.addRow(new Object[]{pair.getKey(), pair.getValue()});
            }
            return result;
        }

        if(selection.equals("*")) {
            if (port.equals(myPort)) {
                MatrixCursor result = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});
                Iterator it = LocalHM.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry pair = (Map.Entry)it.next();
                    result.addRow(new Object[]{pair.getKey(), pair.getValue()});
                }

                String aimport = NextPort;
                while (!aimport.equals(myPort)) {
                    Message m = new Message('Q', port, null, null, "*", null, null);
                    Send(aimport, m);
                    while (retm == null) {
                    }
                    it = retm.ret.entrySet().iterator();
                    while (it.hasNext()) {
                        Map.Entry pair = (Map.Entry) it.next();
                        result.addRow(new Object[]{pair.getKey(), pair.getValue()});
                    }
                    aimport = retm.myport;
                    retm = null;
                }
                return result;
            }
            else {
                Message m = new Message('R', NextPort, null, null, null, null, LocalHM);
                Send(port, m);
                return null;
            }
        }

        if(!isConnected) {
            MatrixCursor result = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});
            result.addRow(new Object[]{selection, LocalHM.get(selection)});
            return result;
        }

        if (LocalHM.containsKey(selection)) {
            if (!myPort.equals(port)) {
                HashMap<String, String> hm = new HashMap<String, String>();
                hm.put(selection, LocalHM.get(selection));
                Message m = new Message('R', port, null, null, selection, null, hm);
                Send(port, m);
                return null;
            }
            MatrixCursor result = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});
            result.addRow(new Object[]{selection, LocalHM.get(selection)});
            return result;
        }
        else {
            Message m = new Message('Q', port, null, null, selection, null, null);
            Send(NextPort, m);
            if (myPort.equals(port)) {
                while (retm == null) { }
            }
            MatrixCursor result = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});
            result.addRow(new Object[]{selection, retm.ret.get(selection)});

            retm = null;
            return result;
        }
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        doDelete(selection);
        return 0;
    }

    private void doDelete(String key) {
        if (key.equals("@")) {
            LocalHM.clear();
            return;
        }

        if (key.equals("*")) {
            if (isConnected && !LocalHM.isEmpty()) {
                Message m = new Message('D', null, null, null, "*", null, null);
                Send(NextPort, m);
            }
            doDelete("@");
            return;
        }

        if (!isConnected) LocalHM.remove(key);

        String theid = "";
        try {
            theid = genHash(key);
        } catch(Exception e) { }
        if (isMyObject(theid)){
            LocalHM.remove(key);
        }
        else {
            Message m = new Message('D', null, null, null, key, null, null);
            Send(NextPort, m);
        }
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }







    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
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












    private void Send(final String port, final Message msg) {
        new Thread(new Runnable() {
            public void run() {
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(port));

                    ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                    oos.writeObject(msg);
                    oos.close();
                    socket.close();
                    Log.d(TAG, "Send succeed.");
                } catch(Exception e) {
                    Log.e(TAG, "Send failed. " + e);
                }
            }
        }).start();
    }

    private class ServerTask extends AsyncTask<ServerSocket, Message, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            try {
                while (true) {
                    Socket socket = serverSocket.accept();
                    ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                    Message m = (Message) ois.readObject();
                    publishProgress(m);
                }
            } catch (Exception e) {
                Log.e(TAG, "Server failed. " + e);
            }
            return null;
        }

        protected void onProgressUpdate(Message...msgs) {
            Message m = msgs[0];
            switch(m.type) {
                case 'J':
                    Log.d(TAG, "do join.");
                    Join(m);
                    break;
                case 'U':
                    Log.d(TAG, "do update.");
                    UpdateN(m);
                    break;
                case 'I':
                    Log.d(TAG, "do insert.");
                    doInsert(m.key, m.val);
                    break;
                case 'Q':
                    Log.d(TAG, "do query.");
                    try {
                        doQuery(m.key, m.myport);
                    } catch(Exception e) {
                        Log.e(TAG, "do query exception: " + e);
                    }
                    break;
                case 'D':
                    Log.d(TAG, "do delete.");
                    doDelete(m.key);
                    break;
                case 'R':
                    Log.d(TAG, "finish query");
                    if (m.ret == null) Log.d(TAG, "finish query exception: ret is null");
                    retm = m;
                    break;
                default:
                    Log.e(TAG, "Unknown tyoe.");
            }

        }
    }




}
