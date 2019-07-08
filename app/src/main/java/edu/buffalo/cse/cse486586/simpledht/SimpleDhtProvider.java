package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MergeCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.StrictMode;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

    static final String TAG = SimpleDhtProvider.class.getSimpleName();

    static final int AVD_PORT_FOR_JOIN = 11108;
    static final int SERVER_PORT = 10000;

    static final String msgChannelReady = "ChannelReady";
    static final String msgJoinCmd = "Join";
    static final String msgCloseCmd = "Close";
    static final String msgSetSuccessor = "SetSuccessor";
    static final String msgSetPredecessor = "SetPredecessor";
    static final String msgSetThisConnectionAsSuccessor = "SetThisConnectionAsSuccessor";
    static final String msgSetThisConnectionAsPredecessor = "SetThisConnectionPredecessor";
    static final String msgInsertCmd = "Insert";
    static final String msgQueryCmd = "Query";
    static final String msgQueryResultCmd = "QueryResult";
    static final String msgQueryResultEndCmd = "QueryResultEnd";
    static final String msgLoopEnd = "LoopEnd";
    static final String msgDeleteCmd = "Delete";

    static String currentProcessNumber = null;

    //DB File
    private static final String DATABASE_NAME = "SimpleDHT.db";

    //Wed on't need the version for this. But just keeping 1 for future like PA2 B (Not Sure)
    private static final int VERSION = 1;
    private SQLiteDatabase mDatabase;

    @Override
    public boolean onCreate() {
        // If you need to perform any one-time initialization task, please do it here.
        Context context = getContext();
        mDatabase = new DHTDBHelper(getContext()).getWritableDatabase();

        if(mDatabase == null)
            return false;

        StrictMode.ThreadPolicy tp = StrictMode.ThreadPolicy.LAX;
        StrictMode.setThreadPolicy(tp);

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        currentProcessNumber = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);

        new HandleJoinNodeTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, SERVER_PORT);
        new CommandHandlerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, Integer.parseInt(currentProcessNumber));

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Log.i(TAG, e.getMessage());
        }

        return true;
    }

    @Override
    public void shutdown() {
        mDatabase.close();
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        String overrideSelection = null;
        String [] overrideSelectionArg = null;

        boolean isSuccessorDeleteNeeded = true;
        boolean isLocalDeleteNeeded = false;
        //Log.i(TAG, "delete with selection: " + selection);
        if(selection != null) {
            if(selection.equals("@")) {
                isSuccessorDeleteNeeded = false;
                isLocalDeleteNeeded =  true;
            }
            else if(selection.equals("*")) {
                isLocalDeleteNeeded = true;
            } else {
                String hashedKey = DHTNodeInfo.genHash(selection);

                //Find if the key needs to insert in this node
                if( (ConnectedNodeInfo.getInstance().getPredecessorNodeHash().compareTo(DHTNodeInfo.genHash(currentProcessNumber)) >= 0 &&
                        (hashedKey.compareTo(ConnectedNodeInfo.getInstance().getPredecessorNodeHash()) > 0 ||
                                hashedKey.compareTo(DHTNodeInfo.genHash(currentProcessNumber)) <= 0))
                        || (hashedKey.compareTo(ConnectedNodeInfo.getInstance().getPredecessorNodeHash()) > 0 &&
                        hashedKey.compareTo(DHTNodeInfo.genHash(currentProcessNumber)) <= 0)) {
                    //If key exists at DB then need to update or else insert
                    isSuccessorDeleteNeeded = false;
                    isLocalDeleteNeeded = true;
                    overrideSelection = DHTDbSchema.MessageTable.Cols.KEY + "= ?";
                    overrideSelectionArg = new String[] { selection };
                }
            }
        }

        if(isSuccessorDeleteNeeded) {
            //Log.i(TAG, "Delete need to be executed for successor. Key: " + selection);
            ConnectedNodeInfo.getInstance().setDeleteRunning(true);
            ConnectedNodeInfo.getInstance().getSuccessorWriter().println(msgDeleteCmd);
            ConnectedNodeInfo.getInstance().getSuccessorWriter().println(selection);
        }

        Cursor cursor = null;
        if(isLocalDeleteNeeded) {
            //Log.i(TAG, "Delete need to be executed in this node for key: " + selection);
            mDatabase.delete(
                    DHTDbSchema.MessageTable.NAME,
                    overrideSelection,
                    overrideSelectionArg
            );
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
        String key = (String) values.get(DHTDbSchema.MessageTable.Cols.KEY);
        String value = (String) values.get(DHTDbSchema.MessageTable.Cols.VALUE);
        String hashedKey = DHTNodeInfo.genHash(key);

        //Find if the key needs to insert in this node
        if( (ConnectedNodeInfo.getInstance().getPredecessorNodeHash().compareTo(DHTNodeInfo.genHash(currentProcessNumber)) >= 0 &&
                (hashedKey.compareTo(ConnectedNodeInfo.getInstance().getPredecessorNodeHash()) > 0 ||
                 hashedKey.compareTo(DHTNodeInfo.genHash(currentProcessNumber)) <= 0))
            || (hashedKey.compareTo(ConnectedNodeInfo.getInstance().getPredecessorNodeHash()) > 0 &&
                hashedKey.compareTo(DHTNodeInfo.genHash(currentProcessNumber)) <= 0)) {
            //If key exists at DB then need to update or else insert
            //Log.i(TAG, "insert at this node");
            boolean isKeyPresent = false;
            Cursor resultCursor = query(uri, null, key, null, null);
            if (resultCursor != null) {
                if (resultCursor.moveToFirst()){
                    do{
                        String returnKey = resultCursor.getString(resultCursor.getColumnIndex(DHTDbSchema.MessageTable.Cols.KEY));
                        //As the query will return the hash value we need match against hash value
                        if (returnKey.equals(key)) {
                            isKeyPresent = true;
                            break;
                        }
                    }while(resultCursor.moveToNext());
                }
                resultCursor.close();
            }
            if(isKeyPresent == true) {
                update(uri, values, key, null);
            } else {
                //Insert the hash code for the key
                //values.put(DHTDbSchema.MessageTable.Cols.KEY, hashedKey);
                mDatabase.insert(DHTDbSchema.MessageTable.NAME, null, values);
                //Log.i("insert", values.toString());
            }
        } else {
            //Log.i(TAG, "insert to successor with key: " + key);
            ConnectedNodeInfo.getInstance().getSuccessorWriter().println(msgInsertCmd);
            ConnectedNodeInfo.getInstance().getSuccessorWriter().println(key);
            ConnectedNodeInfo.getInstance().getSuccessorWriter().println(value);
            if(!ConnectedNodeInfo.getInstance().isInsertFromInternalChordAlgo()) {
                try {
                    Thread.sleep(250);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        return uri;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        ConnectedNodeInfo.getInstance().setQueryRunning(true);
        String overrideSelection = null;
        String [] overrideSelectionArg = null;

        boolean isSuccessorLookupNeeded = true;
        boolean isLocalLookupNeeded = false;
        //Log.i(TAG, "query with selection: " + selection);

        if(selection != null) {
            if(selection.equals("@")) {
                isSuccessorLookupNeeded = false;
                isLocalLookupNeeded =  true;
            }
            else if(selection.equals("*")) {
                isLocalLookupNeeded = true;
            } else {
                String hashedKey = DHTNodeInfo.genHash(selection);

                //Find if the key needs to insert in this node
                if( (ConnectedNodeInfo.getInstance().getPredecessorNodeHash().compareTo(DHTNodeInfo.genHash(currentProcessNumber)) >= 0 &&
                        (hashedKey.compareTo(ConnectedNodeInfo.getInstance().getPredecessorNodeHash()) > 0 ||
                                hashedKey.compareTo(DHTNodeInfo.genHash(currentProcessNumber)) <= 0))
                        || (hashedKey.compareTo(ConnectedNodeInfo.getInstance().getPredecessorNodeHash()) > 0 &&
                        hashedKey.compareTo(DHTNodeInfo.genHash(currentProcessNumber)) <= 0)) {
                    //If key exists at DB then need to update or else insert
                    isSuccessorLookupNeeded = false;
                    isLocalLookupNeeded = true;
                    overrideSelection = DHTDbSchema.MessageTable.Cols.KEY + "= ?";
                    overrideSelectionArg = new String[] { selection };
                }
            }
        }

        if(isSuccessorLookupNeeded) {
            //Log.i(TAG, "query need to be executed for successor. Key: " + selection);
            ConnectedNodeInfo.getInstance().getSuccessorWriter().println(msgQueryCmd);
            ConnectedNodeInfo.getInstance().getSuccessorWriter().println(selection);

            while (!CommandResult.getInstance().isQueryResultReady()) {
                try {
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            //Log.i(TAG, "Fetch local query result.");
        }

        Cursor cursor = null;
        if(isLocalLookupNeeded) {
            //Log.i(TAG, "query need to be executed in this node for key: " + selection);
            cursor = mDatabase.query(
                DHTDbSchema.MessageTable.NAME,
                projection, // columns - null selects all columns
                overrideSelection,
                overrideSelectionArg,
                null, // groupBy
                null, // having
                sortOrder
            );
        }

        // Merge two cursors
        MergeCursor mergeCursor = new MergeCursor(new Cursor[] { CommandResult.getInstance().getQueryResult(), cursor });
        CommandResult.getInstance().getQueryResult().close();
        CommandResult.getInstance().clearQueryResult();
        CommandResult.getInstance().setQueryResultReady(false);
        ConnectedNodeInfo.getInstance().setQueryRunning(false);
        return mergeCursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // Convert the key to hash code and then update
        //values.put(DHTDbSchema.MessageTable.Cols.KEY,
        //        genHash((String)values.get(DHTDbSchema.MessageTable.Cols.KEY)));
        mDatabase.update(DHTDbSchema.MessageTable.NAME, values,
                DHTDbSchema.MessageTable.Cols.KEY + " = ?",
                new String[] { selection });
        return 0;
    }

    //This part of code is inspired by book:
    //Android Programming: The Big Nerd Ranch Guide, Third Edition
    //Android provides the SQLiteOpenHelper class to check if the db is already exists,
    //if it doesn't it creates and create the initial db etc.
    //We need have a derived class from the abstract class
    public class DHTDBHelper extends SQLiteOpenHelper {
        private static final int VERSION = 1;
        private static final String DATABASE_NAME = "crimeBase.db";

        public DHTDBHelper(Context context) {
            super(context, DATABASE_NAME, null, VERSION);
        }

        @Override
        public void onCreate(SQLiteDatabase db) {
            db.execSQL("create table " + DHTDbSchema.MessageTable.NAME + "(" +
                    DHTDbSchema.MessageTable.Cols.KEY + ", " +
                    DHTDbSchema.MessageTable.Cols.VALUE +
                    ")"
            );
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        }
    }

    class DHTDbSchema {
        final class MessageTable {
            static final String NAME = "message";

            final class Cols {
                static final String KEY = "key";
                static final String VALUE = "value";
            }
        }
    }

    private class HandleJoinNodeTask extends AsyncTask<Integer, String, Void> {

        @Override
        protected Void doInBackground(Integer... sockets) {
            Integer serverPort = sockets[0];

            DHTNodeInfo dhtNodeInfo = new DHTNodeInfo();
            BufferedReader predecessorReader = null;

            //Creating a server socket and a thread (AsyncTask) that listens on the server port
            ServerSocket serverSocket = null;
            //Connect to 5554 Server to get the successor and predecessor Info
            while(serverSocket == null) {
                try {
                    serverSocket = new ServerSocket(serverPort);
                    serverSocket.setSoTimeout(10);
                } catch (Exception e) {
                    Log.i(TAG, e.getMessage());
                    serverSocket = null;
                }
            }

            while(!Thread.interrupted()) {
                try {
                    //When a client connects to server, a client socket will be returned
                    Socket clientSocket = serverSocket.accept();

                    //Create a socket reader and writer to get/send msg
                    BufferedReader clientSocketReader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    PrintWriter clientSocketWriter = new PrintWriter(clientSocket.getOutputStream(), true);
                    //Log.i(TAG, "connection accepted from client");

                    //Notify the AVD that the channel is ready.
                    clientSocketWriter.println(msgChannelReady);

                    //Start accepting the input from the connected AVD
                    String cmdMsg = clientSocketReader.readLine();
                    String avdNodeNumber = clientSocketReader.readLine();
                    //Log.i(TAG, "Received command request: " + cmdMsg + " from: " + avdNodeNumber);

                    if(cmdMsg.equals(msgJoinCmd)) {
                        dhtNodeInfo.nodeJoin(avdNodeNumber);
                        //Log.i(TAG, "Replying to " + avdNodeNumber + ". Info - successor: "
                        //        + dhtNodeInfo.getSuccessor(avdNodeNumber)
                        //        + " predecessor: " + dhtNodeInfo.getPredecessor(avdNodeNumber));
                        clientSocketWriter.println(msgSetSuccessor);
                        clientSocketWriter.println(dhtNodeInfo.getSuccessor(avdNodeNumber));
                        clientSocketWriter.println(msgSetPredecessor);
                        clientSocketWriter.println(dhtNodeInfo.getPredecessor(avdNodeNumber));

                        while(!clientSocketReader.readLine().equals(msgCloseCmd)) {
                        }
                        clientSocketReader.close();
                        clientSocketWriter.close();
                        clientSocket.close();
                        continue;
                    }

                    if(cmdMsg.equals(msgSetThisConnectionAsSuccessor)) {
                        //Log.i(TAG, "setting successor as " + avdNodeNumber);
                        ConnectedNodeInfo.getInstance().setSuccessorConnectionStable(false);
                        ConnectedNodeInfo.getInstance().setSuccessorNodeNumber(avdNodeNumber);
                        ConnectedNodeInfo.getInstance().setSuccessorWriter(clientSocketWriter);
                        ConnectedNodeInfo.getInstance().setSuccessorReader(clientSocketReader);
                        ConnectedNodeInfo.getInstance().setSuccessorConnectionStable(true);
                    } else if(cmdMsg.equals(msgSetThisConnectionAsPredecessor)) {
                        //Log.i(TAG, "setting predecessor as " + avdNodeNumber);
                        ConnectedNodeInfo.getInstance().setPredecessorConnectionStable(false);
                        ConnectedNodeInfo.getInstance().setPredecessorNodeNumber(avdNodeNumber);
                        ConnectedNodeInfo.getInstance().setPredecessorWriter(clientSocketWriter);
                        ConnectedNodeInfo.getInstance().setPredecessorReader(clientSocketReader);
                        ConnectedNodeInfo.getInstance().setPredecessorConnectionStable(true);
                    }
                } catch (SocketTimeoutException st) {
                    //Do nothing
                } catch (IOException e) {
                    Log.e(TAG, e.getMessage(), e);
                }

                try {
                    if(ConnectedNodeInfo.getInstance().isSuccessorConnectionStable() &&
                            ConnectedNodeInfo.getInstance().getSuccessorReader() != null &&
                            !CommandResult.getInstance().isQueryResultReady() &&
                            ConnectedNodeInfo.getInstance().getSuccessorReader().ready()) {
                        //Log.i(TAG, "Received result from successor and append this to local result");
                        String resultFromSuccessor = ConnectedNodeInfo.getInstance().getSuccessorReader().readLine();
                        if (msgQueryResultCmd.equals(resultFromSuccessor)) {
                            while (!(resultFromSuccessor = ConnectedNodeInfo.getInstance().getSuccessorReader().readLine()).equals(msgQueryResultEndCmd)) {
                                String valueFromSuccessor = ConnectedNodeInfo.getInstance().getSuccessorReader().readLine();
                                CommandResult.getInstance().addRowToQueryResult(resultFromSuccessor, valueFromSuccessor);
                            }
                            CommandResult.getInstance().setQueryResultReady(true);
                        } else if (msgLoopEnd.equals(resultFromSuccessor)) {
                            //Log.i(TAG, "query reached end of loop from successor");
                            CommandResult.getInstance().setQueryResultReady(true);
                        }
                    }
                } catch (SocketTimeoutException st) {
                    Log.i(TAG, st.getMessage(), st);
                } catch (IOException e) {
                    Log.e(TAG, e.getMessage(), e);
                }
            }
            try {
                serverSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
    }

    private class CommandHandlerTask extends AsyncTask<Integer, String, Void> {

        private Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
        private int mSeqNum = 0;

        @Override
        protected Void doInBackground(Integer... processNumber) {

            //Log.i(TAG, "To join Chord ring, connection to " + AVD_PORT_FOR_JOIN);
            Socket socket = null;
            PrintWriter socketWriter;
            BufferedReader socketReader;

            //Connect to 5554 Server to get the successor and predecessor Info
            int retryCount = 0;
            int avdToConnect = AVD_PORT_FOR_JOIN;
            while(true) {
                try {
                    Thread.sleep(100);
                    //Create a socket writer and reader to send/receive data
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), avdToConnect);
                    socketWriter = new PrintWriter(socket.getOutputStream(), true);
                    socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                    if(msgChannelReady.equals(socketReader.readLine())) {
                        Log.i(TAG, "connected to server");
                        break;
                    } else {
                        socket = null;
                        if(retryCount++ == 3) {
                            //Try reconnecting to itself
                            avdToConnect = Integer.parseInt(currentProcessNumber) * 2;
                        }
                    }
                } catch (Exception e) {
                    Log.i(TAG, e.getMessage());
                    socket = null;
                }
            }

            try {
                Log.i(TAG, "Sending command request: " + msgJoinCmd + " from " + currentProcessNumber);
                //Initiate the join command
                socketWriter.println(msgJoinCmd);
                socketWriter.println(currentProcessNumber);

                //Read the successor and predecessor Info
                String cmdMsg = socketReader.readLine();
                String successor = null;
                if(cmdMsg.equals(msgSetSuccessor)) {
                    successor = socketReader.readLine();
                }

                cmdMsg = socketReader.readLine();
                String predecessor = null;
                if(cmdMsg.equals(msgSetPredecessor)) {
                    predecessor = socketReader.readLine();
                }

                //Terminate the connection
                socketWriter.println(msgCloseCmd);
                socketReader.close();
                socketWriter.close();
                socket.close();

                if(successor == null || predecessor == null) {
                    return null;
                }

                //Log.i(TAG, "Connecting to successor node: " + successor);
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(successor) * 2));
                socketWriter = new PrintWriter(socket.getOutputStream(), true);
                socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                socketWriter.println(msgSetThisConnectionAsPredecessor);
                socketWriter.println(currentProcessNumber);
                ConnectedNodeInfo.getInstance().setSuccessorNodeNumber(successor);
                ConnectedNodeInfo.getInstance().setSuccessorReader(socketReader);
                ConnectedNodeInfo.getInstance().setSuccessorWriter(socketWriter);
                ConnectedNodeInfo.getInstance().setSuccessorConnectionStable(true);

                if(!currentProcessNumber.equals(predecessor)) {
                    //Log.i(TAG, "Connecting to predecessor node: " + predecessor);
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(predecessor) * 2));
                    socketWriter = new PrintWriter(socket.getOutputStream(), true);
                    socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    socketWriter.println(msgSetThisConnectionAsSuccessor);
                    socketWriter.println(currentProcessNumber);
                    ConnectedNodeInfo.getInstance().setPredecessorNodeNumber(predecessor);
                    ConnectedNodeInfo.getInstance().setPredecessorReader(socketReader);
                    ConnectedNodeInfo.getInstance().setPredecessorWriter(socketWriter);
                    ConnectedNodeInfo.getInstance().setPredecessorConnectionStable(true);

                    Thread.sleep(50);
                    ConnectedNodeInfo.getInstance().getSuccessorWriter().println(msgQueryCmd);
                    ConnectedNodeInfo.getInstance().getSuccessorWriter().println("@");
                    ConnectedNodeInfo.getInstance().setNodeJoinCompleted(false);
                }

            } catch (UnknownHostException e) {
                Log.e(TAG, e.getMessage(), e);
            } catch (IOException e) {
                Log.e(TAG, e.getMessage(), e);
            } catch (InterruptedException e) {
                //do nothing
            }

            int idealCount = 10;
            while(!Thread.interrupted()) {
                try {
                    String cmdPredecessor;
                    idealCount = (idealCount + 10) % 50;
                    if(ConnectedNodeInfo.getInstance().getPredecessorReader() != null &&
                        ConnectedNodeInfo.getInstance().getPredecessorReader().ready()) {
                        idealCount = 10;
                        cmdPredecessor = ConnectedNodeInfo.getInstance().getPredecessorReader().readLine();
                        //Log.i(TAG, "Command received from predecessor: " + cmdPredecessor);
                        if (msgInsertCmd.equals(cmdPredecessor)) {
                            ContentValues cv = new ContentValues();
                            String key = ConnectedNodeInfo.getInstance().getPredecessorReader().readLine();
                            String value = ConnectedNodeInfo.getInstance().getPredecessorReader().readLine();
                            cv.put(DHTDbSchema.MessageTable.Cols.KEY, key);
                            cv.put(DHTDbSchema.MessageTable.Cols.VALUE, value);
                            ConnectedNodeInfo.getInstance().setInsertFromInternalChordAlgo(true);
                            insert(mUri, cv);
                            ConnectedNodeInfo.getInstance().setInsertFromInternalChordAlgo(false);
                        } else if(msgQueryCmd.equals(cmdPredecessor)) {
                            String selection = ConnectedNodeInfo.getInstance().getPredecessorReader().readLine();
                            if(ConnectedNodeInfo.getInstance().isQueryRunning()) {
                                //Log.i(TAG, "query reached end of loop. Notify Predecessor.");
                                ConnectedNodeInfo.getInstance().getPredecessorWriter().println(msgLoopEnd);
                                continue;
                            }
                            Cursor resultCursor = query(mUri, null, selection, null, null);
                            //Log.i(TAG, "Back from local query");
                            if (resultCursor != null) {
                                //Log.i(TAG, "Need to send the result from local query to predecessor");
                                ConnectedNodeInfo.getInstance().getPredecessorWriter().println(msgQueryResultCmd);
                                if (resultCursor.moveToFirst()){
                                    do{
                                        String returnKey = resultCursor.getString(resultCursor.getColumnIndex(DHTDbSchema.MessageTable.Cols.KEY));
                                        String returnValue = resultCursor.getString(resultCursor.getColumnIndex(DHTDbSchema.MessageTable.Cols.VALUE));
                                        ConnectedNodeInfo.getInstance().getPredecessorWriter().println(returnKey);
                                        ConnectedNodeInfo.getInstance().getPredecessorWriter().println(returnValue);
                                    }while(resultCursor.moveToNext());
                                }

                                ConnectedNodeInfo.getInstance().getPredecessorWriter().println(msgQueryResultEndCmd);
                                resultCursor.close();
                            }
                        } else if(msgDeleteCmd.equals(cmdPredecessor)) {
                            String selection = ConnectedNodeInfo.getInstance().getPredecessorReader().readLine();
                            if(ConnectedNodeInfo.getInstance().isDeleteRunning()) {
                                //Log.i(TAG, "Delete reached end of loop. Do Nothing.");
                                ConnectedNodeInfo.getInstance().setDeleteRunning(false);
                                continue;
                            }
                            delete(mUri, selection, null);
                            //Log.i(TAG, "Back from local delete query");
                        }

                    }
                    if(!ConnectedNodeInfo.getInstance().isNodeJoinCompleted()) {
                        if (!CommandResult.getInstance().isQueryResultReady()) {
                            Thread.sleep(5);
                            continue;
                        }
                        //Log.i(TAG, "Fetch and Delete all the keys from successor");
                        Cursor resultCursor = CommandResult.getInstance().getQueryResult();
                        CommandResult.getInstance().clearQueryResult();
                        CommandResult.getInstance().setQueryResultReady(false);

                        //Log.i(TAG, "Iterate through all the keys and insert again");
                        if (resultCursor != null) {
                            if (resultCursor.moveToFirst()) {
                                ConnectedNodeInfo.getInstance().getSuccessorWriter().println(msgDeleteCmd);
                                ConnectedNodeInfo.getInstance().getSuccessorWriter().println("@");
                                Thread.sleep(50);
                                do {
                                    String returnKey = resultCursor.getString(resultCursor.getColumnIndex(DHTDbSchema.MessageTable.Cols.KEY));
                                    String returnValue = resultCursor.getString(resultCursor.getColumnIndex(DHTDbSchema.MessageTable.Cols.VALUE));

                                    //Log.i(TAG, "Need to insert key: " + returnKey);
                                    ContentValues cv = new ContentValues();
                                    cv.put(DHTDbSchema.MessageTable.Cols.KEY, returnKey);
                                    cv.put(DHTDbSchema.MessageTable.Cols.VALUE, returnValue);
                                    ConnectedNodeInfo.getInstance().setInsertFromInternalChordAlgo(true);
                                    insert(mUri, cv);
                                    ConnectedNodeInfo.getInstance().setInsertFromInternalChordAlgo(false);
                                } while (resultCursor.moveToNext());
                            }
                        }

                        resultCursor.close();
                        ConnectedNodeInfo.getInstance().setNodeJoinCompleted(true);
                    }
                    Thread.sleep(idealCount);
                } catch (SocketTimeoutException e) {
                    Log.i(TAG, e.getMessage(), e);
                } catch (IOException e) {
                    Log.i(TAG, e.getMessage(), e);
                } catch (Exception e) {
                    Log.e(TAG, e.getMessage(), e);
                }
            }
            try {
                ConnectedNodeInfo.getInstance().getPredecessorWriter().close();
                ConnectedNodeInfo.getInstance().getPredecessorReader().close();
                ConnectedNodeInfo.getInstance().getSuccessorReader().close();
                ConnectedNodeInfo.getInstance().getSuccessorWriter().close();
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
        private Uri buildUri(String scheme, String authority) {
            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.authority(authority);
            uriBuilder.scheme(scheme);
            return uriBuilder.build();
        }
    }
}