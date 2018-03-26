package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.SystemClock;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import java.util.*;

public class SimpleDynamoProvider extends ContentProvider {

	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";
	private static final String TAG = "SimpleDynamo";
	private static final long checktime = 1000;
	private static final long sleeptime = 100;

	static final int SERVER_PORT = 10000;

	RingHelper ringhelper;
	HashMap<String, String> LocalHM;
	int myPort;
	String Nowselection;
	Message retMessage;

	final static int total_num = 5;
	final static int replica_num = 2;





	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {

		if (selection.equals("@")) {
			LocalHM.clear();
		} else if (selection.equals("*")) {
			int sendPort = myPort;
			Message m = new Message('D', myPort, "*", null, null);
			for (int i = 0; i < total_num; ++i) {
				Send(sendPort, m);
				sendPort = ringhelper.getSuccessor(sendPort);
			}
		} else {
			int sendPort = ringhelper.getPort(selection);
			Message m = new Message('D', myPort, selection, null, null);
			for (int i = 0; i < replica_num+1; ++i) {
				Send(sendPort, m);
				sendPort = ringhelper.getSuccessor(sendPort);
			}
		}
		return 0;
	}


	private void DeleteN(Message m) {
		String selection = m.key;
		if (selection.equals("@") || selection.equals("*")) LocalHM.clear();
		else LocalHM.remove(selection);
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {

		synchronized (this) {



			String key = (String)values.get(KEY_FIELD);
			String value = (String) values.get(VALUE_FIELD);
			Message m = new Message('I', myPort, key, value , null);

			ArrayList<Integer> ports = ringhelper.getAlwaysNext3(key);

			for (Integer i : ports) {
				Send(i, m);
			}

			//System.out.print("insert key: " + key);
			//for (Integer i : ports) System.out.print(" " + i);
			//System.out.println(" ");


			return null;
		}

	}

	private void InsertN(Message m) {
			//System.out.println("insert: " + m.key + " " + m.value);
			LocalHM.put(m.key, m.value);
	}

	private void BackupInsertN(Message m) {
		System.out.println("backup...");

		HashMap<String, String> tmp = m.keyvalueset;
		Iterator it = tmp.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry)it.next();
			String key = (String)pair.getKey();
			if (!LocalHM.containsKey(key)) {
				LocalHM.put(key, (String) pair.getValue());
			}
		}

	}




	@Override
	public boolean onCreate() {

		TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = Integer.parseInt(portStr) * 2;

		ringhelper = new RingHelper();
		LocalHM = new HashMap<String, String>();
		retMessage = null;

		Nowselection = null;


		LocalHM.clear();


		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
			return false;
		}


		recover();
		// TODO Auto-generated method stub
		return false;
	}

	private void recover() {

		System.out.println("recover0");
		//Nowselection = "RECOVERY";

		Message m = new Message('Z', myPort, "RECOVERY", null, null);

		int port1 = ringhelper.getPredecessor(myPort);
		Send(port1, m);
		int port2 = ringhelper.getSuccessor(myPort);
		Send(port2, m);
		/*
		boolean tmp1 = RSend(port1, m);
		System.out.println(tmp1);
		if (tmp1) {
			System.out.println("recover1");
			while (retMessage != null) {
				try {
					Thread.sleep(sleeptime);
				} catch (Exception e) { }
			}
			if (retMessage.keyvalueset != null) {
				HashMap<String, String> tmp = retMessage.keyvalueset;
				Iterator<String> it = tmp.keySet().iterator();
				while (it.hasNext()) {
					String key = it.next();
					LocalHM.put(key, tmp.get(key));
				}

			}
			retMessage = null;
		}
		int port2 = ringhelper.getSuccessor(myPort);
		tmp1 = RSend(port2, m);
		System.out.println(tmp1);
		if (tmp1) {
			System.out.println("recover2");
			while (retMessage != null) {
				try {
					Thread.sleep(sleeptime);
				} catch (Exception e) { }
			}
			if (retMessage.keyvalueset != null) {
				HashMap<String, String> tmp = retMessage.keyvalueset;
				Iterator<String> it = tmp.keySet().iterator();
				while (it.hasNext()) {
					String key = it.next();
					LocalHM.put(key, tmp.get(key));
				}
			}
			retMessage = null;
		}
		*/
	}


	private void RecoverZ(Message m) {



			System.out.println(m.fromport + " is recover.");

			//ringhelper.SetRecovery(m.fromport);

			if (LocalHM.isEmpty()) {
				//Send(m.fromport, new Message('B', myPort, null, null, null));
				return;
			}

			int port3 = m.fromport;
			int port4 = ringhelper.getSuccessor(m.fromport);
			int port2 = ringhelper.getPredecessor(m.fromport);
			int port1 = ringhelper.getPredecessor(port2);
			int port5 = ringhelper.getSuccessor(port4);

			if (myPort == port4 || myPort == port5) {
				HashMap<String, String> retmap = new HashMap<String, String>();
				Iterator it = LocalHM.entrySet().iterator();
				while (it.hasNext()) {
					Map.Entry pair = (Map.Entry) it.next();
					String key = (String) pair.getKey();
					int itsport = ringhelper.getPort(key);
					if (itsport == port3 || itsport == port2) {
						retmap.put(key, (String) pair.getValue());
						//Send(port3, new Message('I', myPort, key, (String) pair.getValue(), null));
					}
				}
				Send(port3, new Message('B', myPort, null, null, retmap));
				return;
			}

			if (myPort == port2 || myPort == port1) {
				HashMap<String, String> retmap = new HashMap<String, String>();
				Iterator it = LocalHM.entrySet().iterator();
				while (it.hasNext()) {
					Map.Entry pair = (Map.Entry) it.next();
					String key = (String) pair.getKey();
					int itsport = ringhelper.getPort(key);
					if (itsport == port2 || itsport == port1) {
						retmap.put(key, (String) pair.getValue());
						//Send(port3, new Message('I', myPort, key, (String) pair.getValue(), null));
					}
				}
				Send(port3, new Message('B', myPort, "RECOVERY", null, retmap));
				return;
			}






	}


	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		synchronized (this) {



			if (selection.equals("@")) {
				MatrixCursor result = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});
				Iterator it = LocalHM.entrySet().iterator();
				while (it.hasNext()) {
					Map.Entry pair = (Map.Entry)it.next();
					result.addRow(new Object[]{pair.getKey(), pair.getValue()});
					//System.out.println(pair.getKey() + " " + pair.getValue());
				}
				return result;
			}

			Nowselection = selection;
			//System.out.println("query for " + selection);

			if (selection.equals("*")) {
				MatrixCursor result = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});
				Iterator it = LocalHM.entrySet().iterator();
				while (it.hasNext()) {
					Map.Entry pair = (Map.Entry)it.next();
					result.addRow(new Object[]{pair.getKey(), pair.getValue()});
				}
				int sendPort = myPort;
				for (int i = 0; i < replica_num; ++i) {
					sendPort = ringhelper.getSuccessor(sendPort);
				}
				Message m = new Message('Q', myPort, "*", null, null);

				//boolean tmp2 =
				RSend(sendPort, m);
				//System.out.println(tmp2);

				//if (!tmp2) {
					//System.out.println("query to backup port.");
				Send(ringhelper.getSuccessor(sendPort), m);
				//}
				while (retMessage == null) {
					try {
						Thread.sleep(sleeptime);
					} catch (Exception e) { }
				}
				//System.out.println(retMessage.key);
				//if (retMessage.keyvalueset == null) System.out.println(retMessage.key + " is null.");
				//System.out.println(retMessage.keyvalueset.size());
				HashMap<String, String> tmp = retMessage.keyvalueset;

				it = tmp.entrySet().iterator();
				while (it.hasNext()) {
					Map.Entry pair = (Map.Entry)it.next();
					result.addRow(new Object[]{pair.getKey(), pair.getValue()});
				}

				retMessage = null;
				//Nowselection = null;
				return result;
			}

			Message m = new Message('Q', myPort, selection, null, null);
			ArrayList<Integer> ports = ringhelper.getAlwaysNext3(selection);
			/*
			for (Integer i : ports) {
				Send(i, m);
			}
			*/


			MatrixCursor result = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});

/*
			Timer checkTimer = new Timer();
			checkTimer.schedule(new CheckTimer(ports), checktime);

			while (retqueue.size() < ports.size()) {
				try {
					Thread.sleep(sleeptime);
				} catch(Exception e) { }
			}

			checkTimer.cancel();
*/
			//ArrayList<Message> retqueue2 = new ArrayList<Message>();
			//ArrayList<Message> retqueue3 = new ArrayList<Message>();
			String value = null;

			for (Integer i : ports) {

				if (RSend(i, m)) {
					Timer checkTimer = new Timer();
					checkTimer.schedule(new CheckTimer(i), checktime);
					while (retMessage == null) {
						try {
							Thread.sleep(sleeptime);
						} catch(Exception e) { }
					}
					checkTimer.cancel();
					if (retMessage.value != null) {
						value = retMessage.value;
						retMessage = null;
						break;
					}
					retMessage = null;
				}
			}


			/*
			for (Message msg : retqueue) {

				//try {
				//	System.out.println(msg.type + " " + msg.fromport + " " + msg.value);
				//} catch (Exception e) { }

				if (msg.value != null) retqueue2.add(msg);
				else retqueue3.add(msg);
			}

			if (retqueue2.isEmpty()) {
				retqueue3.clear();
				ports = ringhelper.getNext3(selection);
				for (Integer i : ports) {
					Send(i, m);
				}

				checkTimer = new Timer();
				checkTimer.schedule(new CheckTimer(ports), checktime);


				while (retqueue.size() != ports.size()) { }
				checkTimer.cancel();


				for (Message msg : retqueue) {
					if (msg.value != null) retqueue2.add(msg);
					else retqueue3.add(msg);
				}
			}


			try {
				value = retqueue2.get(0).value;
			} catch (Exception e) {
				Log.d(TAG, "retqueue2 is empty.");
			}




			for (Message msg : retqueue3) {
				Send(msg.fromport, new Message('I', myPort, selection, value, null));
			}

			if (retqueue2.size() == 3) {
				String value0 = retqueue2.get(0).value;
				String value1 = retqueue2.get(1).value;
				String value2 = retqueue2.get(2).value;
				boolean cmp01 = false;
				boolean cmp02 = false;
				boolean cmp12 = false;
				if (value0.equals(value1)) cmp01 = true;
				if (value0.equals(value2)) cmp02 = true;
				if (value1.equals(value1)) cmp12 = true;
				if (!(cmp01 && cmp02 && cmp12)) {
					if (cmp01 == true) {
						Send(retqueue2.get(2).fromport, new Message('I', myPort, selection, value0, null));
					}
					else if (cmp02 == true) {
						Send(retqueue2.get(1).fromport, new Message('I', myPort, selection, value0, null));
					}
					else {
						Send(retqueue2.get(0).fromport, new Message('I', myPort, selection, value1, null));
						value = value1;
					}
				}
			}
*/

			result.addRow(new Object[]{selection, value});

			Nowselection = null;

			return result;
		}




		// TODO Auto-generated method stub

	}


	//private




	private void QueryN(Message m) {



			String selection = m.key;

			if (selection.equals("*")) {
				System.out.println("send return for queryall.");
				Message retm = new Message('R', myPort, selection, null, LocalHM);
				Send(m.fromport, retm);
				return;
			}

			String value = LocalHM.get(selection);


			Send(m.fromport, new Message('R', myPort, selection, value, null));

			//System.out.println("rquery: " + selection + " " + value);



	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {

		return 0;

	}

	private void ReturnQuery(Message msg) {
		if (msg.key.equals(Nowselection) && retMessage == null) {
			retMessage = msg;
		}
		//else if (!ringhelper.isWorking(msg.fromport)) {
		//	ringhelper.SetRecovery(msg.fromport);
		//}
	}


	private void Send(final int port, final Message msg) {
		new Thread(new Runnable() {
			public void run() {


				try {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							port);

					ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
					oos.writeObject(msg);
					oos.close();
					socket.close();
				} catch(Exception e) {
					Log.e(TAG, "Send failed. " + e);
					//ringhelper.SetCorruptPort(port);
				}
			}
		}).start();
	}

	private boolean RSend(int port, Message msg) {
		try {
			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					port);

			ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
			oos.writeObject(msg);
			oos.close();
			socket.close();
		} catch(Exception e) {
			//System.out.println(e.getMessage());
			return false;
			//ringhelper.SetCorruptPort(port);
		}
		return true;
	}

	private class ServerTask extends AsyncTask<ServerSocket, Message, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];

			while (true) {
				try {
					Socket socket = serverSocket.accept();
					ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
					Message m = (Message) ois.readObject();
					publishProgress(m);}
				catch (Exception e) {
					Log.e(TAG, "Server failed. " + e);
				}
			}

			//return null;
		}

		protected void onProgressUpdate(Message...msgs) {


			Message m = msgs[0];

			switch(m.type) {
				case 'I':
					InsertN(m);
					break;
				case 'Q':
					QueryN(m);
					break;
				case 'R':
					ReturnQuery(m);
					//retqueue.add(m);
					break;
				case 'D':
					DeleteN(m);
					break;
				case 'Z':
					RecoverZ(m);
					break;
				case 'B':
					BackupInsertN(m);
					break;

				default:
					Log.e(TAG, "Unknown tyoe.");
			}
		}


	}

	private class CheckTimer extends TimerTask {
		int port;

		public CheckTimer (int port) {
			this.port = port;
		}
		public void run() {

			//Log.d(TAG, "size of retqueue is " + retqueue.size());
			/*
			ArrayList<Integer> tmp = new ArrayList<Integer>();
			for (Integer port : ports) {
				tmp.add(port);
			}
			for (Message m1 : retqueue) {
				for (Integer s : tmp) {
					if (s.equals(m1.fromport)) {
						tmp.remove(s);
						break;
					}
				}
			}
			for (Integer s : tmp) {
				ringhelper.SetCorruptPort(s);
				System.out.println("port " + s + " is down.");

				retqueue.add(new Message('R', s, null, null, null));
			}
			*/
			retMessage = new Message('R', port, null, null, null);
		}
	}




}
