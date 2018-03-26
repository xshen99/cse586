package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.TextView;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;

import java.text.SimpleDateFormat;
import java.util.ArrayList;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 * 
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {

    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final String REMOTE_PORT0 = "11108";
    static final int SERVER_PORT = 10000;
    static int count = 0;

    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    final String URI = "edu.buffalo.cse.cse486586.groupmessenger2.provider";
    final Uri mUri = buildUri("content", URI);

    //private long[] lasttime = new long[5];//System.currentTimeMillis();
    //private boolean[] istmpempty = new boolean[5];
    //private boolean[] iswork = new boolean[5];
    //private boolean issendempty = true;
    //private boolean issendwork = false;

    private ArrayList<String> ports;

    private PriorityQueue<SendUnit> queue;

    HashMap<String, SendUnit> table;
    private int table_count = 0;

    int penum;

    String myPort;

    boolean isWorking = false;

    private HashMap<String, Timer> check1table;
    private HashMap<String, Timer> check2table;



    //private ArrayList<IndexUnit> table;



    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        
        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));
        
        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */

        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }

        //Log.d(TAG, "thread have been created");

        final TextView ed = (TextView) findViewById(R.id.editText1);

        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));

        findViewById(R.id.button4).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                String msg = ed.getText().toString();
                ed.setText("");



                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);

            }
        });

        //initialize
        //for (int i = 0; i < 5; ++i) {
        //    istmpempty[i] = true;
        //    iswork[i] = true;
        //}

        //the queue
        queue = new PriorityQueue<SendUnit>(100, new Comparator<SendUnit>() {
            @Override
            public int compare(SendUnit s1, SendUnit s2) {
                try {
                    int ret = Integer.parseInt(s1.CurrentIndex) - Integer.parseInt(s2.CurrentIndex);
                    if (ret != 0) return ret;
                    return Integer.parseInt(s1.ReceiveProccessor) - Integer.parseInt(s2.ReceiveProccessor);
                } catch(Exception e) {
                    Log.d(TAG, "2: " + e.toString());
                }
                return 0;
            }
        });

        table = new HashMap<String, SendUnit>(100);

        penum = 4;

        new UpdatePro().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, myPort);

        ports = new ArrayList<String>(4);
        String tmp = "11108";
        for (int i = 0; i < 5; ++i)
        {
            if (!tmp.equals(myPort)) ports.add(tmp);
            tmp = Integer.toString(Integer.parseInt(tmp) + 4);
            //Log.d(TAG, tmp);
        }


        check1table = new HashMap<String, Timer>(100);
        check2table = new HashMap<String, Timer>(100);


        //table = new ArrayList<new PriorityQueue<IndexUnit>(5)>(100);
    }


    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }


    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            try {
                while (true) {
                    Socket socket = serverSocket.accept();
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String msg = in.readLine();
                    publishProgress(msg);
                    Log.d(TAG, "a msg is done.");
                }
            } catch (Exception e) {
            }
            return null;
        }


        protected void onProgressUpdate(String... strings) {
            String strReceived = strings[0].trim();

            Log.d(TAG, strReceived);

            String[] tmp = strReceived.split("\\|");
            //key | sendPE | message firsttime
            if (table.containsKey(tmp[0])) {

                SendUnit tmp_su = table.get(tmp[0]);
                if (tmp_su.SendProccessor.equals(myPort)) {
                    Log.d(TAG, "proccess 3");

                    tmp_su.return_count++;
                    Log.d(TAG, tmp_su.CurrentIndex + " " + tmp[1]);

//                    int a = Integer.getInteger(tmp_su.CurrentIndex);
//                    int b = Integer.getInteger(tmp[1]);
//                    int c = a - b;
                    try {
                        if (Integer.getInteger(tmp_su.CurrentIndex) < Integer.getInteger(tmp[1])) {
                            //second time
                            //key | index | ReceivePE
                            tmp_su.CurrentIndex = tmp[1];
                            tmp_su.ReceiveProccessor = tmp[2];
                        }
                    } catch(Exception e) { }

                    if (tmp_su.return_count == ports.size()) {//penum) {make some change


                        new Proccess3Task().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, tmp_su.key);
                        //3rd sending proccess


                    }
                } else {
                    Log.d(TAG, "proccess 4");

                    tmp_su.ReceiveProccessor = tmp[2];
                    tmp_su.CurrentIndex = tmp[1];
                    while (isWorking) { }
                    queue.add(tmp_su);
                    table.remove(tmp_su.key);

                }//3rd receiving proccess


            } else {
                Log.d(TAG, "create table");

                table_count++;
                SendUnit su = new SendUnit(tmp[0], tmp[1], table_count, tmp[2]);
                table.put(su.key, su);


                //create a new thread
                new Proccess2Task().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, su.key);


                //first time
            }
            //create new su
            //do stuff of 3rd proccess


            //add stuff here
            //2nd, 3rd send proccess


            //if (table.isEmpty()) {





            return;

        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        //This part is the 1st sending proccess
        protected Void doInBackground(String... msgs) {
/*
            ContentValues cv = new ContentValues();
            cv.put(KEY_FIELD, "0");
            count++;
            cv.put(VALUE_FIELD, "Hello, world!");
            try {
                getContentResolver().insert(mUri, cv);
            } catch(Exception e) {
                Log.d(TAG, e.toString());
            }
            Log.d(TAG, "insert test passed.");
*/
            String remotePort = REMOTE_PORT0;

            SimpleDateFormat date = new SimpleDateFormat("ss.SSS");
            Calendar cal = Calendar.getInstance();
            String valnow = date.format(cal.getTime());


            table_count++;
            SendUnit su = new SendUnit(valnow, msgs[1],table_count, msgs[0]);
            table.put(su.key, su);


            String msgToSend = valnow + "|" + msgs[1] + "|" + msgs[0];
                //key | sendPE | message
                //This is the structure of the first sending message
            try {
                for (int i = 0; i < 5; ++i)
                {
                    if (!msgs[1].equals(remotePort))
                    {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(remotePort));
                        Log.d(TAG, "start sending " + msgToSend + " to port " + remotePort);
                        PrintWriter out = new PrintWriter(socket.getOutputStream());
                        out.println(msgToSend);
                        out.flush();
                        socket.close();
                        Timer check1Timer = new Timer();
                        check1Timer.schedule(new Check1Timer(su.key, remotePort), 8000);
                        check1table.put(su.key, check1Timer);
                    }
                    remotePort = Integer.toString(Integer.parseInt(remotePort) + 4);
                }


            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }


            return null;
        }
    }

    private class Proccess2Task extends AsyncTask<String, Void, Void> {

        @Override
        //This part is the 1st sending proccess
        protected Void doInBackground(String... msgs) {

            String key = msgs[0];
            SendUnit su = table.get(key);
            try {
                Socket socket3 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt((su.SendProccessor)));
                PrintWriter out = new PrintWriter(socket3.getOutputStream());
                String msgToSend = su.key + "|" + Integer.toString(table_count) + "|" + myPort;
                out.println(msgToSend);
                out.flush();
                socket3.close();
                Log.d(TAG, msgToSend);
                Timer check2Timer = new Timer();
                check2Timer.schedule(new Check2Timer(su.key, su.SendProccessor), 15000);
                check2table.put(su.key, check2Timer);
            } catch(Exception e) { }
            return null;
        }
    }

    private class Proccess3Task extends AsyncTask<String, Void, Void> {

        @Override
        //This part is the 1st sending proccess
        protected Void doInBackground(String... msgs) {

            String key = msgs[0];
            SendUnit su = table.get(key);
            try {
                String remotePort = REMOTE_PORT0;

                String msgToSend = su.key + "|" + su.CurrentIndex + "|" + su.ReceiveProccessor;
                //key | sendPE | message
                //This is the structure of the first sending message
                for (int j = 0; j < 5; ++j) {
                    if (!myPort.equals(remotePort)) {
                        Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt((remotePort)));
                        Log.d(TAG, "start sending " + msgToSend + " to port " + remotePort);
                        PrintWriter out = new PrintWriter(socket2.getOutputStream());
                        out.println(msgToSend);
                        out.flush();
                        socket2.close();
                    }
                    remotePort = Integer.toString(Integer.parseInt(remotePort) + 4);
                }
                while (isWorking) { }
                queue.add(su);
                table.remove(su.key);
            } catch(Exception e) { }
            return null;
        }
    }

    private class UpdatePro extends AsyncTask<String, String, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            //double time1 = 0;

            while(true) {

                if (table.isEmpty() && !queue.isEmpty()) {
                    //if (time1 == 0)
                    //{
                    //    time1 = System.currentTimeMillis();
                    //}
                    //double time2 = System.currentTimeMillis();
                    //if (time2 - time1 > 500) {
                    //    isWorking = true;

                        //empty the queue
                        while (!queue.isEmpty()) {
                            SendUnit su = queue.poll();
                            //Log.d(TAG, "key = " + su.key + " " + "CurrentIndex = " + su.CurrentIndex + " " + su.msg);
                            try {
                                publishProgress(su.msg);
                            } catch (Exception e) {
                            }

                        }
                    //    isWorking = false;
                    //    time1 = 0;
                  //  }
                }
            }

        }

        protected void onProgressUpdate(String...strings) {

            String strReceived = strings[0].trim();
            ContentValues cv = new ContentValues();

            cv.put(KEY_FIELD, Integer.toString(count));
            count++;
            cv.put(VALUE_FIELD, strReceived);
            Log.d(TAG, cv.toString());
            try {

                getContentResolver().insert(mUri, cv);

            } catch(Exception e) {
                Log.d(TAG, e.toString());
            }


            return;
        }
    }

    public class Check1Timer extends TimerTask {
        String key;
        String port;
        public Check1Timer(String key, String port) {
            this.key = key;
            this.port = port;
        }
        public void run() {

            if (table.containsKey(key))
            {
                new Proccess3Task().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, key);
                if (ports.contains(port)) {
                    ports.remove(port);
                    Log.d(TAG, myPort + " remove " + port);
                }
            }
        }
    }

    public class Check2Timer extends TimerTask {
        String key;
        String port;
        public Check2Timer(String key, String port) {
            this.key = key;
            this.port = port;
        }
        public void run() {

            if (table.containsKey(key))
            {
                table.remove(key);
                if (ports.contains(port)) {
                    ports.remove(port);
                    Log.d(TAG, myPort + " remove " + port);
                }
            }
        }
    }



    private class SendUnit {
        String key;
        String msg;
        String SendProccessor;
        String ReceiveProccessor;
        //String AgreedIndex;
        String CurrentIndex;
        int return_count;

        public SendUnit(String key_, String sp, int CurrentIndex_, String msg_)
        {
            key = key_;
            msg = msg_;
            CurrentIndex = Integer.toString(CurrentIndex_);
            SendProccessor = sp;
            return_count = 0;
            ReceiveProccessor = sp;
            //AgreedIndex = Integer.toString(0);
        }
/*
        public void setSendProccessor(String sendProccessor) {
            SendProccessor = sendProccessor;
        }

        public void setReceiveProccessor(String receiveProccessor) {
            ReceiveProccessor = receiveProccessor;
        }

        public void UpdateCurrentIndex(String Index) {
            if (Integer.getInteger(CurrentIndex) < Integer.getInteger(Index)) {
                CurrentIndex = Index;
            }
        }
        */
    }
/*
    private class SendHelper {
        int PEnum = 5;
       // ArrayList<>



        int theFirstOne = 0;


    }
    */
}
