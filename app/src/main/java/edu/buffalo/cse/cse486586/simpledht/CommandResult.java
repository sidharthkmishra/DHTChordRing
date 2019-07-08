package edu.buffalo.cse.cse486586.simpledht;

import android.database.MatrixCursor;

public class CommandResult {
    public static final CommandResult singleInstance = new CommandResult();
    private CommandResult() {
    }

    public static CommandResult getInstance() {
        return singleInstance;
    }

    private boolean isQueryResultReady = false;
    private MatrixCursor matrixCursor = new MatrixCursor(new String[] {
            SimpleDhtProvider.DHTDbSchema.MessageTable.Cols.KEY, SimpleDhtProvider.DHTDbSchema.MessageTable.Cols.VALUE });

    void clearQueryResult() {
        matrixCursor = new MatrixCursor(new String[] {
                SimpleDhtProvider.DHTDbSchema.MessageTable.Cols.KEY, SimpleDhtProvider.DHTDbSchema.MessageTable.Cols.VALUE });
    }

    void addRowToQueryResult(String key, String value) {
        matrixCursor.addRow(new Object[] { key, value });
    }

    MatrixCursor getQueryResult() {
        return matrixCursor;
    }

    boolean isQueryResultReady() {
        return this.isQueryResultReady;
    }

    synchronized void setQueryResultReady(boolean isQueryResultReady) {
        this.isQueryResultReady = isQueryResultReady;
    }
}
