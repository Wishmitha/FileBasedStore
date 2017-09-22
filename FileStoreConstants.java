package org.wso2.andes.store.file;

/**
 * Holds the constant values pertaining to the file store
 */
public class FileStoreConstants {

    public static final String BLOCK_RESTART_INTERVAL = "blockRestartInterval";
    public static final String BLOCK_SIZE = "blockSize";
    public static final String CACHE_SIZE = "cacheSize";
    public static final String MAX_OPEN_FILES = "maxOpenFiles";
    public static final String WRITE_BUFFER_SIZE = "writeBufferSize";

    public static final String BROKER_STORE = "mbstore";

    public static final String CONNECTOR = "::";

    // new suffixes for key schema

    public static final String MESSAGE = "MESSAGE";
    public static final String QUEUE = "QUEUE";
    public static final String MESSAGE_DESTINATION = "MESSAGE_DESTINATION";
    public static final String LAST_QUEUE_ID = "LAST_QUEUE_ID";
    public static final String LAST_MESSAGE_ID = "LAST_MESSAGE_ID";

    // suffixes for mapping of MB_METADATA table

    public static final String QUEUE_ID = "QUEUE_ID";
    public static final String DLC_QUEUE_ID = "DLC_QUEUE_ID";
    public static final String MESSAGE_METADA = "MESSAGE_METADATA";

    // suffixes for mapping of MB_QUEUE_MAPPING table

    public static final String QUEUE_NAME = "QUEUE_NAME";
    public static final String MESSAGE_COUNT = "MESSAGE_COUNT";

    // suffixes for mapping of MB_QUEUE_MAPPING → MB_METADATA tables

    //public static final String MESSAGE_NO = "MESSAGE_NO_";

    // suffixes for mapping of MB_CONTENT table

    public static final String MESSAGE_CONTENT = "MESSAGE_CONTENT";

    // suffixes for mapping of MB_EXPIRATION_DATA table

    public static final String EXPIRATION_TIME = "EXPIRATION_TIME";
    public static final String MESSAGE_ID = "MESSAGE_ID";

    // last queue id

    // dead letter channel

    public static  final String DLC = "deadletterchannel";
    public static final String DEAD_LETTER_CHANNEL = "DLC";
    public static final String DLC_QUEUE_NAME = "DLC_QUEUE_NAME";
    public static final String DLC_MESSAGE_COUNT = "DLC_MESSAGE_COUNT";




    // generates MESSAGE_NO_$no suffix for mapping of MB_QUEUE_MAPPING → MB_METADATA tables

    /*public static String getMessageNoString(long messageNo){
        return MESSAGE_NO + Long.toString(messageNo);
    }*/

    // generate keys with given identifiers and suffixes



}
