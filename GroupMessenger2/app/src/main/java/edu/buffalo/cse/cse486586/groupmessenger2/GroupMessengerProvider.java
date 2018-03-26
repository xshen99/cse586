package edu.buffalo.cse.cse486586.groupmessenger2;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.util.Log;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

/**
 * GroupMessengerProvider is a key-value table. Once again, please note that we do not implement
 * full support for SQL as a usual ContentProvider does. We re-purpose ContentProvider's interface
 * to use it as a key-value table.
 * 
 * Please read:
 * 
 * http://developer.android.com/guide/topics/providers/content-providers.html
 * http://developer.android.com/reference/android/content/ContentProvider.html
 * 
 * before you start to get yourself familiarized with ContentProvider.
 * 
 * There are two methods you need to implement---insert() and query(). Others are optional and
 * will not be tested.
 * 
 * @author stevko
 *
 */
public class GroupMessengerProvider extends ContentProvider {

    static final String filename_key = "GroupMessenger_key";
    static final String filename_value = "GroupMessenger_value";
    static final String TAG = "GroupMessengerProvider";


    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // You do not need to implement this.
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // You do not need to implement this.
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        /*
         * TODO: You need to implement this method. Note that values will have two columns (a key
         * column and a value column) and one row that contains the actual (key, value) pair to be
         * inserted.
         * 
         * For actual storage, you can use any option. If you know how to use SQL, then you can use
         * SQLite. But this is not a requirement. You can use other storage options, such as the
         * internal storage option that we used in PA1. If you want to use that option, please
         * take a look at the code for PA1.
         */

        String key = (String) values.get(KEY_FIELD);
        String val = (String) values.get(VALUE_FIELD) + "\n";

        String line = "";
        int i = 0;
        try {
            FileInputStream fis = getContext().openFileInput(filename_key);
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));



            while ((line = br.readLine()) != null) {
                ++i;
                if (line.equals(key)) {
                    //Log.d(TAG, "I found " + key + " :-)");
                    break;
                }
            }
            fis.close();
        }catch(Exception e) {}

        if (line == null) {
            try {
                FileOutputStream fos = getContext().openFileOutput(filename_key, Context.MODE_APPEND);
                key += "\n";
                fos.write(key.getBytes());
                fos.close();
                fos = getContext().openFileOutput(filename_value, Context.MODE_APPEND);
                fos.write(val.getBytes());
                fos.close();
            } catch (Exception e) { }
        }



        else {
            //Log.d(TAG, "I'm updating");
            ArrayList<String> lines = new ArrayList<String>();
            int j = 0;
            try {
                FileInputStream fis = getContext().openFileInput(filename_value);
                BufferedReader br = new BufferedReader(new InputStreamReader(fis));


                while ((line = br.readLine()) != null) {
                    //Log.d(TAG, line);
                    ++j;
                    lines.add(line);
                    //else lines.add(val);
                }
                lines.set(i-1, val);
                fis.close();
            } catch (Exception e) {}
            Log.d(TAG, "The length of the lines is " + Integer.toString(j));
            try {
                String newline = "";
                FileOutputStream fos = getContext().openFileOutput(filename_value, Context.MODE_PRIVATE);
                fos.write(newline.getBytes());
                fos.close();

                fos = getContext().openFileOutput(filename_value, Context.MODE_APPEND);
                for (int k = 0; k < j; ++k) {
                    if (k == i-1) newline = lines.get(k);// + "\n";
                    else newline = lines.get(k) + "\n";
                    fos.write(newline.getBytes());
                }

                fos.close();

            } catch (Exception e) {}

        }


        Log.v("insert", values.toString());
        return uri;
    }

    @Override
    public boolean onCreate() {
        try {
            String init = "";
            FileOutputStream fos = getContext().openFileOutput(filename_key, Context.MODE_PRIVATE);
            fos.write(init.getBytes());
            fos.close();
            fos = getContext().openFileOutput(filename_value, Context.MODE_PRIVATE);
            fos.write(init.getBytes());
            fos.close();
        } catch (Exception e) {
            Log.d(TAG, "initation failed");
        }
        // If you need to perform any one-time initialization task, please do it here.
        return false;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // You do not need to implement this.
        return 0;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        /*
         * TODO: You need to implement this method. Note that you need to return a Cursor object
         * with the right format. If the formatting is not correct, then it is not going to work.
         *
         * If you use SQLite, whatever is returned from SQLite is a Cursor object. However, you
         * still need to be careful because the formatting might still be incorrect.
         *
         * If you use a file storage option, then it is your job to build a Cursor * object. I
         * recommend building a MatrixCursor described at:
         * http://developer.android.com/reference/android/database/MatrixCursor.html
         */

        try {
            FileInputStream fis = getContext().openFileInput(filename_key);
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            String line;
            int i = 0;
            while ((line = br.readLine())!= null)
            {
                ++i;
                if (line.equals(selection)) {
                    Log.d(TAG, "I found " + selection + " :-)");
                    break;
                }
            }
            fis.close();
            if (line != null) {
                fis = getContext().openFileInput(filename_value);
                br = new BufferedReader(new InputStreamReader(fis));
                for (int j = 0; j < i; ++j)
                {
                    line = br.readLine();
                }
                Log.d(TAG, "The value I found is " + line + ".");
                MatrixCursor result = new MatrixCursor(new String[]{"key", "value"});
                result.addRow(new Object[]{selection, line});
                return result;

            }
            else {
                Log.d(TAG, "I can't find " + selection + " :( so sad");
                return null;
            }


        } catch (Exception e) { }

        Log.v("query", selection);
        return null;
    }
}
