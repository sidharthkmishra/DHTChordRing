package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

import edu.buffalo.cse.cse486586.simpledht.DHTNodeInfo;

public class ConnectedNodeInfo {

    private BufferedReader mPredecessorReader = null;
    private BufferedReader mSuccessorReader = null;
    private PrintWriter mPredecessorWriter = null;
    private PrintWriter mSuccessorWriter = null;
    
    private String mPredecessorNodeNumber;
    private String mSuccessorNodeNumber;

    private boolean isQueryRunning = false;
    private boolean isDeleteRunning = false;

    private boolean isNodeJoinCompleted = true;

    private boolean isSuccessorConnectionStable = false;
    private boolean isPredecessorConnectionStable = false;

    private boolean isInsertFromInternalChordAlgo = false;

    public static final ConnectedNodeInfo singleInstance = new ConnectedNodeInfo();

    private ConnectedNodeInfo() {
    }

    public static ConnectedNodeInfo getInstance() {
        return singleInstance;
    }

    public boolean isInsertFromInternalChordAlgo() {
        return isInsertFromInternalChordAlgo;
    }

    public void setInsertFromInternalChordAlgo(boolean insertFromInternalChordAlgo) {
        isInsertFromInternalChordAlgo = insertFromInternalChordAlgo;
    }

    public boolean isSuccessorConnectionStable() {
        return isSuccessorConnectionStable;
    }

    public boolean isNodeJoinCompleted() {
        return this.isNodeJoinCompleted;
    }

    public boolean isPredecessorConnectionStable() {
        return isPredecessorConnectionStable;
    }

    synchronized public void setSuccessorConnectionStable(boolean successorConnectionStable) {
        isSuccessorConnectionStable = successorConnectionStable;
    }

    synchronized public void setPredecessorConnectionStable(boolean predecessorConnectionStable) {
        isPredecessorConnectionStable = predecessorConnectionStable;
    }

    synchronized public void setNodeJoinCompleted(boolean isNodeJoinCompleted) {
        this.isNodeJoinCompleted = isNodeJoinCompleted;
    }

    synchronized public boolean isQueryRunning() {
        return isQueryRunning;
    }

    synchronized public void setQueryRunning(boolean queryRunning) {
        this.isQueryRunning = queryRunning;
    }

    synchronized public boolean isDeleteRunning() {
        return isDeleteRunning;
    }

    synchronized public void setDeleteRunning(boolean deleteRunning) {
        this.isDeleteRunning = deleteRunning;
    }

    public String getPredecessorNodeHash() {
    	return DHTNodeInfo.genHash(this.mPredecessorNodeNumber);
    }
    
    public String getSuccessorNodeHash() {
    	return DHTNodeInfo.genHash(this.mSuccessorNodeNumber);
    }

    public BufferedReader getPredecessorReader() {
        while(!isPredecessorConnectionStable) {
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return mPredecessorReader;
    }

    public BufferedReader getSuccessorReader() {
        while(!isSuccessorConnectionStable) {
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return mSuccessorReader;
    }

    public PrintWriter getPredecessorWriter() {
        while(!isPredecessorConnectionStable) {
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return mPredecessorWriter;
    }

    public PrintWriter getSuccessorWriter() {
        while(!isSuccessorConnectionStable) {
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return mSuccessorWriter;
    }

    public void setPredecessorNodeNumber(String predecessorNodeNumber) {
        this.mPredecessorNodeNumber = predecessorNodeNumber;
    }

    public void setSuccessorNodeNumber(String successorNodeNumber) {
        this.mSuccessorNodeNumber = successorNodeNumber;
    }

    synchronized public void setPredecessorReader(BufferedReader predecessorReader) {
        this.mPredecessorReader = predecessorReader;
    }

    synchronized public void setSuccessorReader(BufferedReader successorReader) {
        this.mSuccessorReader = successorReader;
    }

    synchronized public void setPredecessorWriter(PrintWriter predecessorWriter) {
        this.mPredecessorWriter = predecessorWriter;
    }

    synchronized public void setSuccessorWriter(PrintWriter successorWriter) {
        this.mSuccessorWriter = successorWriter;
    }

}
