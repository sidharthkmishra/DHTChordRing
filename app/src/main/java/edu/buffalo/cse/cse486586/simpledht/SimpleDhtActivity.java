package edu.buffalo.cse.cse486586.simpledht;

import android.database.Cursor;
import android.net.Uri;
import android.os.Bundle;
import android.app.Activity;
import android.text.method.ScrollingMovementMethod;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.TextView;

public class SimpleDhtActivity extends Activity {

    private Button mLButton;
    private Button mRButton;
    private TextView mShowMsg;

    static final String TAG = SimpleDhtActivity.class.getSimpleName();

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_simple_dht_main);

        mShowMsg = (TextView) findViewById(R.id.textView1);
        mShowMsg.setMovementMethod(new ScrollingMovementMethod());
        findViewById(R.id.button3).setOnClickListener(
                new OnTestClickListener(mShowMsg, getContentResolver()));

        mLButton = (Button) findViewById(R.id.button1);
        //Setting Listener for LDump
        mLButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                mShowMsg.setText("");
                Uri.Builder uriBuilder = new Uri.Builder();
                uriBuilder.authority("edu.buffalo.cse.cse486586.simpledynamo.provider");
                uriBuilder.scheme("content");

                Cursor resultCursor = getContentResolver().query(uriBuilder.build(), null, "@", null, null);
                if (resultCursor != null) {
                    if (resultCursor.moveToFirst()){
                        do{
                            String returnKey = resultCursor.getString(resultCursor.getColumnIndex(SimpleDhtProvider.DHTDbSchema.MessageTable.Cols.KEY));
                            String returnValue = resultCursor.getString(resultCursor.getColumnIndex(SimpleDhtProvider.DHTDbSchema.MessageTable.Cols.VALUE));
                            mShowMsg.append(returnKey + " : " + returnValue + "\n");
                        }while(resultCursor.moveToNext());
                    }
                    resultCursor.close();
                }
            }
        });

        mRButton = (Button) findViewById(R.id.button2);
        mRButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                mShowMsg.setText("");
                Uri.Builder uriBuilder = new Uri.Builder();
                uriBuilder.authority("edu.buffalo.cse.cse486586.simpledht.provider");
                uriBuilder.scheme("content");

                Cursor resultCursor = getContentResolver().query(uriBuilder.build(), null, "*", null, null);
                if (resultCursor != null) {
                    if (resultCursor.moveToFirst()){
                        do{
                            String returnKey = resultCursor.getString(resultCursor.getColumnIndex(SimpleDhtProvider.DHTDbSchema.MessageTable.Cols.KEY));
                            String returnValue = resultCursor.getString(resultCursor.getColumnIndex(SimpleDhtProvider.DHTDbSchema.MessageTable.Cols.VALUE));
                            mShowMsg.append(returnKey + " : " + returnValue + "\n");
                        }while(resultCursor.moveToNext());
                    }
                    resultCursor.close();
                }
            }
        });

    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_simple_dht_main, menu);
        return true;
    }

}
