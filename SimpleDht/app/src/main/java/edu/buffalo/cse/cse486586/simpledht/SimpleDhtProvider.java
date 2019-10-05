package edu.buffalo.cse.cse486586.simpledht;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;

import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {
    static final String TAG = SimpleDhtActivity.class.getSimpleName();
    String masterNode = "5554";
    String myPort;
    String portStr;
    String predID = null;
    String succID = null;
    String nodeID = null;
    static final int SERVER_PORT = 10000;
    List<String> nodesConnected = new ArrayList<String>();
    HashMap<String, String> encodedPort = new HashMap<String, String>();
    final Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht");
    SQL_Helper chordSQLHelper;
    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
    @Override
    public boolean onCreate() {

        //Generating Port numbers
        // TODO Auto-generated method stub
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        chordSQLHelper = new SQL_Helper(getContext());
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            Log.e("ServerCreated","Server is created");
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            e.printStackTrace();
        }
        //If the AVD is 5554
        if (portStr.equals("5554")){
            //Master node 5554 Code
            Log.e("MasterNode","This is masterNode");
            try {
                encodedPort.put(genHash(portStr), portStr);
                nodesConnected.add(genHash(portStr));
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        } //For all the other AVDs
        else {
            //All other nodes except Master node
            String msg = "Request-";
            //Request a connection to the masterNode
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);
            Log.e("ClientCreated","Client is created");
        }
        return false;
    }
    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        String hashID = null;
        String newSelection = "key = "+"'"+selection+"'";                                              // This is the key which is to be queried in the form of 'key = selection'
        System.out.println(selection);
        System.out.println(newSelection);
        SQLiteDatabase messenger_db = chordSQLHelper.getReadableDatabase();

        //Creates a Readable Database where we have inserted the key, value pairs.
        // Deleting keys for local AVDs
        if (selection.equals("@")) {
            Log.e("DeleteAllRows", "Getting all rows");
            messenger_db.rawQuery("Delete from " + FeedReaderContract.FeedEntry.TABLE_NAME, null);
        } else if (selection.equals("*")) {    //Deleting for all the AVDs
            if (predID != null) {

                    messenger_db.rawQuery("Delete from " + FeedReaderContract.FeedEntry.TABLE_NAME, null);
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),      //Creating a socket to deliver the message.
                                Integer.parseInt(succID) * 2);
                        //socket.setSoTimeout(5000);
                        DataOutputStream queryStream = new DataOutputStream(socket.getOutputStream());
                        queryStream.writeUTF("DeleteAll-" + nodeID);
                        //queryStream.close();
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {
                messenger_db.rawQuery("Delete from " + FeedReaderContract.FeedEntry.TABLE_NAME, null);
            }
        }else if (predID != null){              //If its not for a single AVD
            try {
                hashID = genHash(selection);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            try {
                if (genHash(predID).compareTo(genHash(nodeID)) < 0) {
                    if ((hashID.compareTo(genHash(predID)) > 0) && (hashID.compareTo(genHash(nodeID)) <= 0)) {

                        int d = messenger_db.delete(FeedReaderContract.FeedEntry.TABLE_NAME, newSelection, null);
                        Log.e("Delete", "Count Deleted " + d);
                    } else {
                        Log.e("Delete", "Key is not in this node " + selection + " " +nodeID + " " + succID);
                        try {
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),      //Creating a socket to deliver the message.
                                    Integer.parseInt(succID) * 2);
                            //socket.setSoTimeout(5000);
                            DataOutputStream queryStream = new DataOutputStream(socket.getOutputStream());
                            queryStream.writeUTF("Delete-" + selection);
                            //queryStream.close();
                        } catch (UnknownHostException e) {
                            e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                    }
                }else {
                    if(((hashID.compareTo(genHash(predID)) > 0) && hashID.compareTo(genHash(nodeID)) > 0) || ((hashID.compareTo(genHash(predID)) < 0) && hashID.compareTo(genHash(nodeID)) < 0)) {

                        int d = messenger_db.delete(FeedReaderContract.FeedEntry.TABLE_NAME, newSelection, null);
                        Log.e("DeleteFNSp", "Count Deleted " + d);
                    } else {
                        Log.e("DNotInNodeSpecial", "Key is not in this node " + newSelection + " " + nodeID + " " + succID);
                        try {
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),      //Creating a socket to deliver the message.
                                    Integer.parseInt(succID) * 2);
                            DataOutputStream insertStream = new DataOutputStream(socket.getOutputStream());
                            insertStream.writeUTF("Delete-" + selection);
                            //insertStream.close();

                        } catch (UnknownHostException e) {
                            e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            } catch (NoSuchAlgorithmException e ) {
                e.printStackTrace();
            }
        }
         else {                 //Delete for single avd
             Log.e("DeleteFN", "Deleting from the same node " + newSelection);
             int d = messenger_db.delete(FeedReaderContract.FeedEntry.TABLE_NAME, newSelection, null);
             Log.e("DeleteFN", "Count Deleted " + d);
        }

        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        String hashID = null;
        try {
            hashID = genHash(values.getAsString("key"));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        Log.e("HashKey", "Hashed Key: " + hashID + " " + predID + " " + nodeID + " " + succID);
        //https://developer.android.com/training/data-storage/sqlite.html                               //Used to create a Database using SQLite.
        if(predID != null){
            try {
                if (genHash(predID).compareTo(genHash(nodeID)) < 0) {
                    if ((hashID.compareTo(genHash(predID)) > 0) && (hashID.compareTo(genHash(nodeID)) <= 0)) {
                        SQLiteDatabase messenger_db = chordSQLHelper.getWritableDatabase();                         //This creates a Database through which we can save the key, value paris.
                        long _id = messenger_db.insert(FeedReaderContract.FeedEntry.TABLE_NAME, null, values);   //Used to insert into the the table with the content values

                        Log.e("insertWithin", values.toString());
                        uri = ContentUris.withAppendedId(uri, _id);
                    } else {
                        Log.e("NotInNode", "Key is not in this node " + values.getAsString("key") + " " +nodeID + " " + succID);
                        try {
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),      //Creating a socket to deliver the message.
                                    Integer.parseInt(succID) * 2);
                            DataOutputStream insertStream = new DataOutputStream(socket.getOutputStream());
                            insertStream.writeUTF("Insert-" + values.getAsString("key") + "-" + values.getAsString("value"));
                            //insertStream.close();
                            Log.e("InsertOutside", "Inserted at node " + values.getAsString("key") + values.getAsString("value"));
                        } catch (UnknownHostException e) {
                            e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                    }
                }else {
                    if(((hashID.compareTo(genHash(predID)) > 0) && hashID.compareTo(genHash(nodeID)) > 0) || ((hashID.compareTo(genHash(predID)) < 0) && hashID.compareTo(genHash(nodeID)) < 0)) {
                        SQLiteDatabase messenger_db = chordSQLHelper.getWritableDatabase();                         //This creates a Database through which we can save the key, value paris.
                        long _id = messenger_db.insert(FeedReaderContract.FeedEntry.TABLE_NAME, null, values);   //Used to insert into the the table with the content values

                        Log.e("insertWithinSpecial", values.toString());
                        uri = ContentUris.withAppendedId(uri, _id);
                    } else {
                        Log.e("NotInNodeSpecial", "Key is not in this node " + values.getAsString("key") + " " + nodeID + " " + succID);
                        try {
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),      //Creating a socket to deliver the message.
                                    Integer.parseInt(succID) * 2);
                            DataOutputStream insertStream = new DataOutputStream(socket.getOutputStream());
                            insertStream.writeUTF("Insert-" + values.getAsString("key") + "-" + values.getAsString("value"));
                            //insertStream.close();
                            Log.e("InsertOutside", "Inserted at node " + values.getAsString("key") + values.getAsString("value"));
                        } catch (UnknownHostException e) {
                            e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            } catch (NoSuchAlgorithmException e ) {
                e.printStackTrace();
            }
        }
        else {
            SQLiteDatabase messenger_db = chordSQLHelper.getWritableDatabase();                         //This creates a Database through which we can save the key, value paris.
            long _id = messenger_db.insert(FeedReaderContract.FeedEntry.TABLE_NAME, null, values);   //Used to insert into the the table with the content values
            Log.e("insertSingle", values.toString());
            uri = ContentUris.withAppendedId(uri, _id);
        }
        return uri;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        //https://developer.android.com/training/data-storage/sqlite.html
        String hashID = null;
        String newSelection = "key = "+"'"+selection+"'";                                              // This is the key which is to be queried in the form of 'key = selection'
        System.out.println(selection);
        System.out.println(newSelection);
        SQLiteDatabase messenger_db = chordSQLHelper.getReadableDatabase();
        Cursor cursor = null;
        MatrixCursor matCursor = new MatrixCursor(new String[]{"key","value"});

        //Creates a Readable Database where we have inserted the key, value pairs.

        if (selection.equals("@")){
            Log.e("QueryAllRows", "Getting all rows");
            cursor = messenger_db.rawQuery("select * from "+FeedReaderContract.FeedEntry.TABLE_NAME,null);
        } else if (selection.equals("*")) {
            if (predID != null) {
                Cursor locCur = messenger_db.rawQuery("select * from " + FeedReaderContract.FeedEntry.TABLE_NAME, null);
                int numRows = locCur.getCount();
                try {
                    //https://stackoverflow.com/questions/10723770/whats-the-best-way-to-iterate-an-android-cursor
                    while (locCur.moveToNext()) {
                        String[] keyValPair = {locCur.getString(0), locCur.getString(1)};
                        matCursor.addRow(keyValPair);
                    }
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),      //Creating a socket to deliver the message.
                            Integer.parseInt(succID) * 2);
                    socket.setSoTimeout(5000);
                    DataOutputStream queryStream = new DataOutputStream(socket.getOutputStream());
                    queryStream.writeUTF("QueryAll-" + nodeID);
                    //queryStream.close();

                    Log.e("WaitingQ", "Waiting for cursor");
                    ObjectInputStream ackqueryStream = new ObjectInputStream(socket.getInputStream());
                    List<String> succCur = (ArrayList<String>) ackqueryStream.readObject();
                    Log.e("SuccList", succCur+"");
                    for (int i=0; i<succCur.size(); i++){
                        matCursor.addRow(succCur.get(i).split("-"));
                    }
                    cursor = matCursor;

                } catch (UnknownHostException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            } else {
                cursor = messenger_db.rawQuery("select * from "+FeedReaderContract.FeedEntry.TABLE_NAME,null);

            }
        } else if (predID != null){
            try {
                hashID = genHash(selection);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
                try {
                    if (genHash(predID).compareTo(genHash(nodeID)) < 0) {
                        if ((hashID.compareTo(genHash(predID)) > 0) && (hashID.compareTo(genHash(nodeID)) <= 0)) {
                            cursor = messenger_db.query(FeedReaderContract.FeedEntry.TABLE_NAME, projection, newSelection, null, null, null, sortOrder);
                        } else {
                            Log.e("QNotInNode", "Key is not in this node " + selection + " " +nodeID + " " + succID);
                            try {
                                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),      //Creating a socket to deliver the message.
                                        Integer.parseInt(succID) * 2);
                                socket.setSoTimeout(5000);
                                DataOutputStream queryStream = new DataOutputStream(socket.getOutputStream());
                                queryStream.writeUTF("Query-" + selection);
                                //queryStream.close();

                                Log.e("WaitingQ", "Waiting for cursor");
                                DataInputStream ackqueryStream = new DataInputStream(socket.getInputStream());
                                String[] values = ackqueryStream.readUTF().split("-");
                                //ackqueryStream.close();
                                //socket.close();

                                matCursor.addRow(values);
                                cursor = matCursor;
                                Log.e("QueryOutside", "Queryed " + values);
                                //return matCursor;
                            } catch (UnknownHostException e) {
                                e.printStackTrace();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }

                        }
                    }else {
                        if(((hashID.compareTo(genHash(predID)) > 0) && hashID.compareTo(genHash(nodeID)) > 0) || ((hashID.compareTo(genHash(predID)) < 0) && hashID.compareTo(genHash(nodeID)) < 0)) {
                            cursor = messenger_db.query(FeedReaderContract.FeedEntry.TABLE_NAME, projection, newSelection, null, null, null, sortOrder);

                        } else {
                            Log.e("QNotInNodeSpecial", "Key is not in this node " + newSelection + " " + nodeID + " " + succID);
                            try {
                                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),      //Creating a socket to deliver the message.
                                        Integer.parseInt(succID) * 2);
                                DataOutputStream insertStream = new DataOutputStream(socket.getOutputStream());
                                insertStream.writeUTF("Query-" + selection);
                                //insertStream.close();

                                DataInputStream ackinsertStream = new DataInputStream(socket.getInputStream());
                                String[] values = ackinsertStream.readUTF().split("-");
                                //ackinsertStream.close();
                                //socket.close();
                                matCursor.addRow(values);
                                cursor = matCursor;
                                Log.e("QueryOutsideS", "Queryed" + values);
                                return matCursor;
                            } catch (UnknownHostException e) {
                                e.printStackTrace();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                } catch (NoSuchAlgorithmException e ) {
                    e.printStackTrace();
                }
            } else {
                cursor = messenger_db.query(FeedReaderContract.FeedEntry.TABLE_NAME, projection, newSelection, null, null, null, sortOrder);
        }
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
        Log.v("query", selection);
        // TODO Auto-generated method stub
        return cursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... serverSockets) {
            ServerSocket serverSocket = serverSockets[0];
            try{
                while(true) {
                    Socket clientSocket = serverSocket.accept();
                    DataInputStream inStream = new DataInputStream(clientSocket.getInputStream());
                    String[] msgReceived = inStream.readUTF().split("-");
                    String type = msgReceived[0];
                    if (type.equals("Request")) {
                        Log.e("Adding Node", "Node " + msgReceived[1] + " is added to list");
                        encodedPort.put(genHash(msgReceived[1]), msgReceived[1]);
                        nodesConnected.add(genHash(msgReceived[1]));
                        Collections.sort(nodesConnected);
                        int size = nodesConnected.size();
                        for (int i = 0; i < size; i++) {
                            //https://stackoverflow.com/questions/5385024/mod-in-java-produces-negative-numbers
                            int predNodeNum = ((i - 1) < 0) ? (size - (Math.abs(i - 1) % size)) % size : ((i - 1) % size);
                            int succNodeNum = (i + 1) % size;
                            String port = encodedPort.get(nodesConnected.get(i));
                            if (port.equals(masterNode)) {
                                Log.e("MasterUpdate", "Updating master predID and succID");
                                predID = encodedPort.get(nodesConnected.get(predNodeNum));
                                succID = encodedPort.get(nodesConnected.get(succNodeNum));
                                nodeID = encodedPort.get(nodesConnected.get(i));
                            } else {
                                Log.e("UpdateOther", "Updating " + port + " port.");
                                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),      //Creating a socket to deliver the message.
                                        Integer.parseInt(port) * 2);
                                DataOutputStream sendPS = new DataOutputStream(socket.getOutputStream());
                                String msgToSend = "Update-" + encodedPort.get(nodesConnected.get(i)) + "-" + encodedPort.get(nodesConnected.get(predNodeNum)) + "-" + encodedPort.get(nodesConnected.get(succNodeNum));
                                sendPS.writeUTF(msgToSend);
                                Log.e("UpdateOther", "Sending Updates");
                                DataInputStream ackPS = new DataInputStream(socket.getInputStream());
                                String aPS = ackPS.readUTF();
                                Log.e("UpdateOther", "Acknowledgement Received");

                                sendPS.close();
                                ackPS.close();
                                socket.close();
                            }

                        }
                        for (String node : nodesConnected) {
                            Log.e("Nodes", "Connected nodes " + node);
                        }
                        DataOutputStream outStream = new DataOutputStream(clientSocket.getOutputStream());
                        outStream.writeUTF("ackRequest");
                        //outStream.close();
                    } else if (type.equals("Update")) {
                        nodeID = msgReceived[1];
                        predID = msgReceived[2];
                        succID = msgReceived[3];
                        Log.e("NodeUpdate", "Updating node predID and succID " + predID + " " + nodeID + " " + succID);
                        DataOutputStream outPS = new DataOutputStream(clientSocket.getOutputStream());
                        outPS.writeUTF("ackRequest");
                        outPS.close();

                    } else if (type.equals("Insert")) {
                        String hashID = genHash(msgReceived[1]);
                        ContentValues values = new ContentValues();
                        values.put("key", msgReceived[1]);
                        values.put("value", msgReceived[2]);
                        if (genHash(predID).compareTo(genHash(nodeID)) < 0) {
                            if ((hashID.compareTo(genHash(predID)) > 0) && (hashID.compareTo(genHash(nodeID)) <= 0)) {
                                SQLiteDatabase messenger_db = chordSQLHelper.getWritableDatabase();                         //This creates a Database through which we can save the key, value paris.
                                long _id = messenger_db.insert(FeedReaderContract.FeedEntry.TABLE_NAME, null, values);   //Used to insert into the the table with the content values

                                Log.e("InsertWithinServer", values.toString());

                            } else {
                                Log.e("NotInNodeServer", "Key is not in this node " + msgReceived[1] + " " + nodeID + " " + succID);
                                try {
                                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),      //Creating a socket to deliver the message.
                                            Integer.parseInt(succID) * 2);
                                    DataOutputStream insertStream = new DataOutputStream(socket.getOutputStream());
                                    insertStream.writeUTF("Insert-" + values.getAsString("key") + "-" + values.getAsString("value"));
                                    //insertStream.close();

                                    Log.e("InsertOutsideServer", "Inserted at node " + values.getAsString("key") + values.getAsString("value"));
                                } catch (UnknownHostException e) {
                                    e.printStackTrace();
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }

                            }
                        } else {
                            if (((hashID.compareTo(genHash(predID)) > 0) && hashID.compareTo(genHash(nodeID)) > 0) || ((hashID.compareTo(genHash(predID)) < 0) && hashID.compareTo(genHash(nodeID)) < 0)) {
                                SQLiteDatabase messenger_db = chordSQLHelper.getWritableDatabase();                         //This creates a Database through which we can save the key, value paris.
                                long _id = messenger_db.insert(FeedReaderContract.FeedEntry.TABLE_NAME, null, values);   //Used to insert into the the table with the content values

                                Log.e("insertWithinSpecialS", values.toString());
                            } else {
                                Log.e("NotInNodeSpecialS", "Key is not in this node " + msgReceived[1] + " " + nodeID + " " + succID);
                                try {
                                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),      //Creating a socket to deliver the message.
                                            Integer.parseInt(succID) * 2);
                                    DataOutputStream insertStream = new DataOutputStream(socket.getOutputStream());
                                    insertStream.writeUTF("Insert-" + values.getAsString("key") + "-" + values.getAsString("value"));
                                    //insertStream.close();

                                } catch (UnknownHostException e) {
                                    e.printStackTrace();
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                        DataOutputStream ackinsertStream = new DataOutputStream(clientSocket.getOutputStream());
                        ackinsertStream.writeUTF("Ack");
                        Log.e("InsertOutsideServer", "Ack" + "Inserted at node " + values.getAsString("key") + values.getAsString("value"));
                        //ackinsertStream.close();
                        //clientSocket.close();

                    } else if (type.equals("Query")) {
                        String hashID = null;
                        String selection = msgReceived[1];
                        String newSelection = "key = " + "'" + selection + "'";
                        Cursor cursor = null;
                        MatrixCursor matCursor = new MatrixCursor(new String[]{"key", "value"});
                        SQLiteDatabase messenger_db = chordSQLHelper.getWritableDatabase();
                        Log.e("InQueryServer", hashID + " " + selection);
                        try {
                            hashID = genHash(selection);
                        } catch (NoSuchAlgorithmException e) {
                            e.printStackTrace();
                        }
                        Log.e("InQuery", "Hash " + hashID);
                        try {
                            if (genHash(predID).compareTo(genHash(nodeID)) < 0) {
                                if ((hashID.compareTo(genHash(predID)) > 0) && (hashID.compareTo(genHash(nodeID)) <= 0)) {
                                    Log.e("Query1", "Getting Cursor");
                                    cursor = messenger_db.query(FeedReaderContract.FeedEntry.TABLE_NAME, null, newSelection, null, null, null, null);

                                } else {
                                    Log.e("NotInNode", "Key is not in this node " + selection + " " + nodeID + " " + succID);
                                    try {
                                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),      //Creating a socket to deliver the message.
                                                Integer.parseInt(succID) * 2);
                                        DataOutputStream insertStream = new DataOutputStream(socket.getOutputStream());
                                        insertStream.writeUTF("Query-" + selection);
                                        //insertStream.close();

                                        DataInputStream ackinsertStream = new DataInputStream(socket.getInputStream());
                                        String[] values = ackinsertStream.readUTF().split("-");
                                        ackinsertStream.close();
                                        //socket.close();

                                        matCursor.addRow(values);
                                        cursor = matCursor;
                                        Log.e("QueryOutside", "Queryed " + values);

                                    } catch (UnknownHostException e) {
                                        e.printStackTrace();
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }

                                }
                            } else {
                                if (((hashID.compareTo(genHash(predID)) > 0) && hashID.compareTo(genHash(nodeID)) > 0) || ((hashID.compareTo(genHash(predID)) < 0) && hashID.compareTo(genHash(nodeID)) < 0)) {
                                    Log.e("Query2", "Getting Cursor");
                                    cursor = messenger_db.query(FeedReaderContract.FeedEntry.TABLE_NAME, null, newSelection, null, null, null, null);

                                } else {
                                    Log.e("QNotInNodeSpecial", "Key is not in this node " + selection + " " + nodeID + " " + succID);
                                    try {
                                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),      //Creating a socket to deliver the message.
                                                Integer.parseInt(succID) * 2);
                                        DataOutputStream insertStream = new DataOutputStream(socket.getOutputStream());
                                        insertStream.writeUTF("Query-" + selection);
                                        //insertStream.close();

                                        DataInputStream ackinsertStream = new DataInputStream(socket.getInputStream());
                                        String[] values = ackinsertStream.readUTF().split("-");
                                        ackinsertStream.close();
                                        //socket.close();
                                        matCursor.addRow(values);
                                        cursor = matCursor;

                                    } catch (UnknownHostException e) {
                                        e.printStackTrace();
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }
                                }
                            }
                        } catch (NoSuchAlgorithmException e) {
                            e.printStackTrace();
                        }
                        DataOutputStream ackOutStream = new DataOutputStream(clientSocket.getOutputStream());
                        if (cursor.getCount() > 0) {
                            cursor.moveToFirst();
                        }
                        Log.e("QueryFound", "Values is " + cursor.getString(0) + "-" + cursor.getString(1));
                        ackOutStream.writeUTF(cursor.getString(0) + "-" + cursor.getString(1));
                        Log.e("QuerySent", "Sent value " + cursor.getString(0));
                        //ackOutStream.close();
                        //clientSocket.close();
                    } else if (type.equals("QueryAll")) {
                        SQLiteDatabase messenger_db = chordSQLHelper.getReadableDatabase();
                        Cursor locCur = null;
                        List<String> rows = new ArrayList<String>();
                        String callingNode = msgReceived[1];
                        if (!callingNode.equals(nodeID)) {
                            try {
                                locCur = messenger_db.rawQuery("select * from " + FeedReaderContract.FeedEntry.TABLE_NAME, null);
                                while (locCur.moveToNext()) {
                                    Log.e("AddingCurRow", "Adding row " + locCur.getString(0) + "-" + locCur.getString(1));
                                    rows.add(locCur.getString(0) + "-" + locCur.getString(1));
                                }
                                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),      //Creating a socket to deliver the message.
                                        Integer.parseInt(succID) * 2);
                                DataOutputStream insertStream = new DataOutputStream(socket.getOutputStream());
                                insertStream.writeUTF("QueryAll-" + callingNode);
                                //insertStream.close();

                                ObjectInputStream ackinsertStream = new ObjectInputStream(socket.getInputStream());
                                List<String> succRows = (ArrayList<String>) ackinsertStream.readObject();
                                for (int i = 0; i < succRows.size(); i++) {
                                    Log.e("AddingSuccRow", "Adding row " + succRows.get(i));
                                    rows.add(succRows.get(i));
                                }
                            } catch (ClassNotFoundException e) {
                                e.printStackTrace();
                            }
                        }

                        ObjectOutputStream objStream = new ObjectOutputStream(clientSocket.getOutputStream());
                        objStream.writeObject(rows);
                    } else if (type.equals("Delete-")) {
                        String hashID = null;
                        String selection = msgReceived[1];
                        String newSelection = "key = " + "'" + selection + "'";                                              // This is the key which is to be queried in the form of 'key = selection'
                        System.out.println(selection);
                        System.out.println(newSelection);
                        SQLiteDatabase messenger_db = chordSQLHelper.getReadableDatabase();
                        try {
                            hashID = genHash(selection);
                        } catch (NoSuchAlgorithmException e) {
                            e.printStackTrace();
                        }
                        try {
                            if (genHash(predID).compareTo(genHash(nodeID)) < 0) {
                                if ((hashID.compareTo(genHash(predID)) > 0) && (hashID.compareTo(genHash(nodeID)) <= 0)) {

                                    int d = messenger_db.delete(FeedReaderContract.FeedEntry.TABLE_NAME, newSelection, null);
                                    Log.e("DeleteFNSer", "Count Deleted " + d);
                                } else {
                                    Log.e("DNotInNodeSer", "Key is not in this node " + selection + " " + nodeID + " " + succID);
                                    try {
                                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),      //Creating a socket to deliver the message.
                                                Integer.parseInt(succID) * 2);
                                        socket.setSoTimeout(5000);
                                        DataOutputStream queryStream = new DataOutputStream(socket.getOutputStream());
                                        queryStream.writeUTF("Delete-" + selection);

                                    } catch (UnknownHostException e) {
                                        e.printStackTrace();
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }

                                }
                            } else {
                                if (((hashID.compareTo(genHash(predID)) > 0) && hashID.compareTo(genHash(nodeID)) > 0) || ((hashID.compareTo(genHash(predID)) < 0) && hashID.compareTo(genHash(nodeID)) < 0)) {
                                    int d = messenger_db.delete(FeedReaderContract.FeedEntry.TABLE_NAME, newSelection, null);
                                    Log.e("DeleteFNSer", "Count Deleted " + d);
                                } else {
                                    Log.e("DNotInNodeSpecial", "Key is not in this node " + newSelection + " " + nodeID + " " + succID);
                                    try {
                                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),      //Creating a socket to deliver the message.
                                                Integer.parseInt(succID) * 2);
                                        DataOutputStream insertStream = new DataOutputStream(socket.getOutputStream());
                                        insertStream.writeUTF("Delete-" + selection);
                                        //insertStream.close();

                                    } catch (UnknownHostException e) {
                                        e.printStackTrace();
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }
                                }
                            }
                            DataOutputStream ackStreamDelete = new DataOutputStream(clientSocket.getOutputStream());
                            ackStreamDelete.writeUTF("DeleteAck1");
                        } catch (IOException e) {
                            e.printStackTrace();
                        } catch (NoSuchAlgorithmException e) {
                            e.printStackTrace();
                        }

                    } else if (type.equals("DeleteAll-")) {

                        SQLiteDatabase messenger_db = chordSQLHelper.getReadableDatabase();
                        String callingNode = msgReceived[1];
                        if(!callingNode.equals(nodeID)) {
                            try {
                                messenger_db.rawQuery("Delete from " + FeedReaderContract.FeedEntry.TABLE_NAME, null);

                                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),      //Creating a socket to deliver the message.
                                        Integer.parseInt(succID) * 2);
                                DataOutputStream insertStream = new DataOutputStream(socket.getOutputStream());
                                insertStream.writeUTF("DeleteAll-" + msgReceived[1]);
                            } catch (UnknownHostException e) {
                                e.printStackTrace();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                        DataOutputStream ackStreamDelete = new DataOutputStream(clientSocket.getOutputStream());
                        ackStreamDelete.writeUTF("DeleteAck2");
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (NoSuchAlgorithmException e ) {
                e.printStackTrace();
            }
            return null;
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void>{

        @Override
        protected Void doInBackground(String... msgs) {
            String[] msg = msgs[0].split("-");
            String type = msg[0];
            if (type.equals("Request")) {
                try {
                    Log.e("ClientRequest","Sending Request to "+Integer.parseInt(masterNode)*2);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),      //Creating a socket to deliver the message.
                            Integer.parseInt(masterNode)*2);
                    socket.setSoTimeout(5000);
                    String msgToSend = type + "-" + portStr;
                    DataOutputStream outStream = new DataOutputStream(socket.getOutputStream());
                    outStream.writeUTF(msgToSend);

                    DataInputStream inStream = new DataInputStream(socket.getInputStream());
                    String ackRequest = inStream.readUTF();
                    //inStream.close();
                    //socket.close();
                    Log.e("AckRequest","Received Ack");
                } catch (IOException e) {
                    Log.e("No Socket", masterNode + " not started.");
                    masterNode = portStr;
                    nodeID = portStr;
                }
            }
            return null;
        }
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

/*
References :
1. https://developer.android.com/training/data-storage/sqlite.html
2. https://stackoverflow.com/questions/10723770/whats-the-best-way-to-iterate-an-android-cursor
3. https://stackoverflow.com/questions/5385024/mod-in-java-produces-negative-numbers
4. https://cse.buffalo.edu/~stevko/courses/cse486/spring19/lectures/15-dht.pdf
 */