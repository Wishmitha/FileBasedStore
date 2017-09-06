package org.wso2.andes.store.file;

import org.apache.log4j.Logger;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.WriteBatch;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by wishmitha on 9/1/17.
 */
public class Transaction {

    private WriteBatch batch;
    boolean isCommited;

    private static final Logger log = Logger.getLogger(Transaction.class);

    private Map<String, byte[]> updates = new HashMap<String, byte[]>();

    public Transaction(DB db){
        this.batch = db.createWriteBatch();
    }

    public void close() {
        try {
            this.batch.close();
        } catch (IOException e) {
            log.error("Error occured while closing ",e);
        }
    }

    public void commit(DB db){
        db.write(this.batch);
        this.isCommited = true;
    }

    public void rollback(DB db) {
        this.updates = new HashMap<String, byte[]>();
        this.batch = db.createWriteBatch();
        this.isCommited = false;
    }

    public void setKey(String key, byte[] value){
        updates.put(key,value);
        batch.put(key.getBytes(),value);
    }

    public byte[] getKey(String key){
        return updates.get(key);
    }

    public void put(byte[] key, byte[] value){
        this.batch.put(key,value);
    }

    public void delete(byte[] key){
        this.batch.delete(key);
    }

}
