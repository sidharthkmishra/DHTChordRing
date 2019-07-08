package edu.buffalo.cse.cse486586.simpledht;

import android.util.Log;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.LinkedList;
import java.util.ListIterator;

public class DHTNodeInfo {
    private static final String TAG = DHTNodeInfo.class.getSimpleName();
    private LinkedList<String> mDHTNodeList = new LinkedList<String>();

    public static String genHash(String input) {
        String hashCode = input;
        try {
            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            byte[] sha1Hash = sha1.digest(input.getBytes());
            Formatter formatter = new Formatter();
            for (byte b : sha1Hash) {
                formatter.format("%02x", b);
            }
            hashCode = formatter.toString();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            Log.v(TAG, e.getMessage(), e);
        }

        return hashCode;
    }

    public String getPredecessor(String node) {
        String predecessor = mDHTNodeList.getLast();
        ListIterator<String> dhtIter = mDHTNodeList.listIterator();
        while(dhtIter.hasNext()) {
            String nodeAtDHT = dhtIter.next();
            if(genHash(nodeAtDHT).compareTo(genHash(node)) == 0) {
                dhtIter.previous();
                break;
            }
        }
        if(dhtIter.hasPrevious())
            predecessor = dhtIter.previous();

        return predecessor;
    }

    public String getSuccessor(String node) {
        String successor = mDHTNodeList.getFirst();
        ListIterator<String> dhtIter = mDHTNodeList.listIterator();
        while(dhtIter.hasNext()) {
            String nodeAtDHT = dhtIter.next();
            if(genHash(nodeAtDHT).compareTo(genHash(node)) == 0)
                break;
        }
        if(dhtIter.hasNext())
            successor = dhtIter.next();

        return successor;
    }

    public void nodeJoin(String node) {
        int insertPos = 0;
        for(String nodeAtDHT : mDHTNodeList) {
            //If the node to be inserted hash is smaller than nodeAtDHT
            //then break and insert
            if(genHash(node).compareTo(genHash(nodeAtDHT)) < 0)
                break;

            insertPos++;
        }
        mDHTNodeList.add(insertPos, node);
    }
}
