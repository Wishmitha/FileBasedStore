/*
 * Copyright (c) 2005-2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.store.file;

import com.gs.collections.api.iterator.MutableLongIterator;
import com.gs.collections.impl.list.mutable.primitive.LongArrayList;
import com.gs.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.apache.log4j.Logger;
import org.iq80.leveldb.*;
import org.python.antlr.op.In;
import org.wso2.andes.configuration.util.ConfigurationProperties;
import org.wso2.andes.dtx.XidImpl;
import org.wso2.andes.kernel.AndesContextStore;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.AndesMessagePart;
import org.wso2.andes.kernel.DeliverableAndesMetadata;
import org.wso2.andes.kernel.DtxStore;
import org.wso2.andes.kernel.DurableStoreConnection;
import org.wso2.andes.kernel.MessageStore;
import org.wso2.andes.kernel.dtx.AndesPreparedMessageMetadata;
import org.wso2.andes.kernel.dtx.DtxBranch;

import java.io.File;
import java.io.IOException;
import java.util.*;

import javax.transaction.xa.Xid;

import static org.iq80.leveldb.impl.Iq80DBFactory.asString;
import static org.iq80.leveldb.impl.Iq80DBFactory.bytes;
import static org.iq80.leveldb.impl.Iq80DBFactory.factory;


/**
 * Implementation of LevelDB broker store. Message persistence related methods are implemented
 * in this class.
 */
public class FileMessageStoreImpl implements MessageStore {

    private static final Logger log = Logger.getLogger(FileMessageStoreImpl.class);

    /**
     * All the database operations are preformed on this object. Refers to the LevelDB store.
     */
    private DB brokerStore;

    /**
     * Used to handle the last queue id.
     */
    private int lastQueueID;

    /**
     * Used to handle the message count in dead letter channel.
     */
    private long dlcMessageCount;


    /**
     * {@inheritDoc} Check if data can be inserted, read and finally deleted
     * from the database.
     */
    @Override
    public boolean isOperational(String testString, long testTime) {
        return false;
    }

    /**
     * {@inheritDoc} Check if data can be inserted, read and finally deleted
     * from the database.
     */
    @Override
    public DurableStoreConnection initializeMessageStore(AndesContextStore contextStore, ConfigurationProperties
            storeProperties) throws AndesException {
        try {
            int mb = 1048576;
            Options options = new Options();

            options.blockRestartInterval(Integer.parseInt(storeProperties.getProperty(FileStoreConstants.
                    BLOCK_RESTART_INTERVAL)));
            options.blockSize(Integer.parseInt(storeProperties.getProperty(FileStoreConstants.BLOCK_SIZE)));
            options.cacheSize(Integer.parseInt(storeProperties.getProperty(FileStoreConstants.CACHE_SIZE)) * mb);
            options.maxOpenFiles(Integer.parseInt(storeProperties.getProperty(FileStoreConstants.MAX_OPEN_FILES)) * mb);
            options.writeBufferSize(Integer.parseInt(storeProperties.getProperty(FileStoreConstants.WRITE_BUFFER_SIZE))
                    * mb);
            options.createIfMissing(true);

            brokerStore = factory.open(new File(FileStoreConstants.BROKER_STORE), options);
            brokerStore.put(bytes(FileStoreConstants.LAST_MESSAGE_ID), bytes("1"));
            brokerStore.put(bytes(FileStoreConstants.DLC_MESSAGE_COUNT), bytes("0"));

        } catch (IOException e) {
            log.error("LevelDB store initiation failed.", e);
        }

        return null;
    }

    @Override
    public void storeMessagePart(List<AndesMessagePart> partList) throws AndesException { // same as add content to batch

    }

    @Override //(Implemented)
    public AndesMessagePart getContent(long messageId, int offsetValue) throws AndesException {

        String messageContentIdentifier = generateKey(FileStoreConstants.MESSAGE_CONTENT, FileStoreConstants.MESSAGE, Long.toString(messageId), Integer.toString(offsetValue));

        byte[] messageContent = brokerStore.get(bytes(messageContentIdentifier));

        AndesMessagePart messagePart = new AndesMessagePart();
        messagePart.setData(messageContent);
        messagePart.setMessageID(messageId);
        messagePart.setOffSet(offsetValue);

        return messagePart;

    }

    @Override //(Implemented)
    public LongObjectHashMap<List<AndesMessagePart>> getContent(LongArrayList messageIDList) throws AndesException {  //TODO get content for other offsets.

        long currentTime = System.currentTimeMillis();
        int numberOfMessages = messageIDList.size();

        MutableLongIterator mutableLongIterator = messageIDList.longIterator();
        LongObjectHashMap<List<AndesMessagePart>> messages = new LongObjectHashMap<>();

        while (mutableLongIterator.hasNext()) {

            long messageID = mutableLongIterator.next();

            List<AndesMessagePart> messageContentList = new ArrayList<>();

            DBIterator keyIterator = brokerStore.iterator();
            String head = generateKey(FileStoreConstants.MESSAGE_CONTENT, FileStoreConstants.MESSAGE, Long.toString(messageID), "0");
            keyIterator.seek(bytes(head));

            try {

                while (keyIterator.hasNext()) {

                    String key = asString(keyIterator.peekNext().getKey());
                    String[] keySplit = key.split(FileStoreConstants.CONNECTOR);
                    Long currentID = Long.parseLong(keySplit[1]);
                    String identifier = keySplit[keySplit.length - 1];

                    if (currentID != messageID || !identifier.equals(FileStoreConstants.MESSAGE_CONTENT)) {
                        break;
                    }

                    int offset = Integer.parseInt(keySplit[2]);

                    byte[] messageContent = brokerStore.get(bytes(key));

                    AndesMessagePart messagePart = new AndesMessagePart();
                    messagePart.setData(messageContent);
                    messagePart.setMessageID(messageID);
                    messagePart.setOffSet(offset);

                    messageContentList.add(messagePart);

                    keyIterator.next();

                }

            } finally {

                try {

                    keyIterator.close();

                } catch (IOException e) {

                    log.error("Error occured while closing ", e);

                }

            }

            messages.put(messageID, messageContentList);
            //messageContentList.clear();

        }

        long processedTime = System.currentTimeMillis();

        log.debug("No. of Messages :" + numberOfMessages + " | Time taken " + (processedTime - currentTime) + "ms");

        return messages;

    }

    @Override //(Implemented)
    public void storeMessages(List<AndesMessage> messageList) throws AndesException {

        Transaction tx = new Transaction(brokerStore);

        try {

            for (AndesMessage message : messageList) {
                storeMessage(message, tx);
            }

            tx.commit(brokerStore);

        } catch (AndesException e) {
            tx.close();
            log.error("Messages storing failed", e);
        } catch (DBException e) {
            tx.close();
            log.error("Messages storing failed", e);
        } finally {
            tx.close();
        }

    }

    // added
    private void storeMessage(AndesMessage message, Transaction tx) throws AndesException {

        AndesMessageMetadata metadata = message.getMetadata();

        //MB_METADATA table

        addMetadataToBatch(metadata, metadata.getStorageQueueName(), tx);

        //MB_EXPIRATION_DATA

        if (metadata.isExpirationDefined()) {
            addExpiryTableEntryToBatch(metadata, tx);
        }

        //MB_CONTENT table

        for (AndesMessagePart messagePart : message.getContentChunkList()) {
            addContentToBatch(messagePart, tx);
        }

        // update last message ID

        tx.put(bytes(FileStoreConstants.LAST_MESSAGE_ID), bytes(Long.toString(metadata.getMessageID())));
    }

    // added
    void addMetadataToBatch(AndesMessageMetadata metadata, final String queueName, Transaction tx) throws AndesException {

        //preparing keys
        String queueIDIdentifier = generateKey(FileStoreConstants.QUEUE_ID, FileStoreConstants.MESSAGE, Long.toString(metadata.getMessageID()));
        String dlcQueueIDIdentifier = generateKey(FileStoreConstants.DLC_QUEUE_ID, FileStoreConstants.MESSAGE, Long.toString(metadata.getMessageID()));
        String messageMetaDataIdentifier = generateKey(FileStoreConstants.MESSAGE_METADA, FileStoreConstants.MESSAGE, Long.toString(metadata.getMessageID()));

        //inserting values and adding to the batch
        tx.put(bytes(queueIDIdentifier), bytes(getQueueID(queueName))); //TODO not implemented how queues are stored
        tx.put(bytes(dlcQueueIDIdentifier), bytes("-1"));
        tx.put(bytes(messageMetaDataIdentifier), metadata.getMetadata());

        createQueueMetaDataMapping(metadata, queueName, tx);

    }

    // added
    void addExpiryTableEntryToBatch(AndesMessageMetadata metadata, Transaction tx) {

        //preparing keys
        String messageDestinationExpirationTimeIdentifier = generateKey(FileStoreConstants.EXPIRATION_TIME, FileStoreConstants.MESSAGE, Long.toString(metadata.getMessageID()), metadata.getStorageQueueName());
        String destinationMessageExpirationTimeIdentifier = generateKey(FileStoreConstants.EXPIRATION_TIME, FileStoreConstants.MESSAGE_DESTINATION, metadata.getStorageQueueName(), Long.toString(metadata.getMessageID()));
        //TODO "$message_destination.$expiration_time.MESSAGE_ID : message_id" key-value relation is not implemented

        //inserting values and adding to the batch
        tx.put(bytes(messageDestinationExpirationTimeIdentifier), bytes(Long.toString(metadata.getExpirationTime())));
        tx.put(bytes(destinationMessageExpirationTimeIdentifier), bytes(Long.toString(metadata.getExpirationTime())));

    }

    // added
    void addContentToBatch(AndesMessagePart messagePart, Transaction tx) {

        //preparing keys
        String messageContentIdentifier = generateKey(FileStoreConstants.MESSAGE_CONTENT, FileStoreConstants.MESSAGE, Long.toString(messagePart.getMessageID()), Long.toString(messagePart.getOffset()));

        //inserting values and adding to the batch
        tx.put(bytes(messageContentIdentifier), messagePart.getData());

    }

    // added
    void createQueueMetaDataMapping(AndesMessageMetadata metadata, final String queueName, Transaction tx) {

        //preparing keys
        String queueMessageMetaDataIdentifier = generateKey(FileStoreConstants.MESSAGE_METADA, FileStoreConstants.QUEUE, queueName, Long.toString(metadata.getMessageID()));
        String queueMessageCountIdentifier = generateKey(FileStoreConstants.MESSAGE_COUNT, FileStoreConstants.QUEUE, queueName);

        Long queueMessageCount;

        if (asString(tx.getKey(queueMessageCountIdentifier)) == null) {
            queueMessageCount = Long.parseLong(asString(brokerStore.get(bytes(queueMessageCountIdentifier))));
        } else {
            queueMessageCount = Long.parseLong(asString(tx.getKey(queueMessageCountIdentifier)));
        }

        queueMessageCount++;

        tx.setKey(queueMessageCountIdentifier, bytes(Long.toString(queueMessageCount)));

        tx.put(bytes(queueMessageMetaDataIdentifier), metadata.getMetadata());
        tx.put(bytes(queueMessageCountIdentifier), bytes(Long.toString(queueMessageCount)));
    }

    // added
    private void updateLastQueueID() {

        String lastQueueID = asString(brokerStore.get(bytes(FileStoreConstants.LAST_QUEUE_ID)));

        if (lastQueueID == null) {
            lastQueueID = "0";
        }

        this.lastQueueID = Integer.parseInt(lastQueueID);

    }

    // added
    private String getQueueID(String queueName) throws AndesException {

        String queueIDIdentifier = generateKey(FileStoreConstants.QUEUE_ID, FileStoreConstants.QUEUE, queueName);
        String queueID = asString(brokerStore.get(bytes(queueIDIdentifier)));

        if (queueID == null) {
            addQueue(queueName);
            queueID = asString(brokerStore.get(bytes(queueIDIdentifier)));
        }

        return queueID;
    }

    //added
    private String getQueueName(long queueID) {
        String queueNameIdentifier = generateKey(FileStoreConstants.QUEUE_NAME, FileStoreConstants.QUEUE, Long.toString(queueID));
        return asString(brokerStore.get(bytes(queueNameIdentifier)));
    }

    @Override //(Implemented)
    public void moveMetadataToQueue(long messageId, String currentQueueName, String targetQueueName) throws AndesException {

        Transaction tx = new Transaction(brokerStore);

        try {

            //updating queue_id in message metadata
            String messageQueueIDIdentifier = generateKey(FileStoreConstants.QUEUE_ID, FileStoreConstants.MESSAGE, Long.toString(messageId));
            tx.put(bytes(messageQueueIDIdentifier), bytes(getQueueID(targetQueueName)));

            // delete message from current queue
            deleteMessageFromQueue(messageId, currentQueueName, tx);

            // add message to the target queue
            String messageMetaDataIdentifier = generateKey(FileStoreConstants.MESSAGE_METADA, FileStoreConstants.MESSAGE, Long.toString(messageId));
            byte[] byteMetaData = brokerStore.get(bytes(messageMetaDataIdentifier));
            AndesMessageMetadata metadata = new AndesMessageMetadata();
            metadata.setMessageID(messageId);
            metadata.setMetadata(byteMetaData);
            createQueueMetaDataMapping(metadata, targetQueueName, tx);

            tx.commit(brokerStore);

        } catch (DBException e) {
            tx.close();
            log.error("Message :" + messageId + " moving from " + currentQueueName + " to " + targetQueueName + " failed.", e);
        } finally {
            tx.close();
        }
    }

    @Override //(Implemented)
    public void moveMetadataToDLC(long messageId, String dlcQueueName) throws AndesException {

        Transaction tx = new Transaction(brokerStore);

        try {

            // create deadletter channel if not already exisits
            String dlcQueueID = getQueueID(dlcQueueName);

            // add message metadata to dlc queue
            String messageMetaDataIdentifier = generateKey(FileStoreConstants.MESSAGE_METADA, FileStoreConstants.MESSAGE, Long.toString(messageId));

            AndesMessageMetadata metadata = new AndesMessageMetadata();
            metadata.setMessageID(messageId);
            metadata.setMetadata(brokerStore.get(bytes(messageMetaDataIdentifier)));

            createQueueMetaDataMapping(metadata, dlcQueueName, tx);

            // update message dlc queue id
            String messageDLCQueueIDIdentifier = generateKey(FileStoreConstants.DLC_QUEUE_ID, FileStoreConstants.MESSAGE, Long.toString(messageId));
            tx.put(bytes(messageDLCQueueIDIdentifier), bytes(dlcQueueID));

            updateDLCMessageCount();

            // delete metadata from current queue
            String messageQueueIdIdentifier = generateKey(FileStoreConstants.QUEUE_ID, FileStoreConstants.MESSAGE, Long.toString(messageId));
            long queueID = Long.parseLong(asString(brokerStore.get(bytes(messageQueueIdIdentifier))));
            deleteMessageFromQueue(messageId, getQueueName(queueID), tx);

            tx.commit(brokerStore);

        } catch (DBException e) {
            tx.close();
            log.error("Message :" + messageId + " moving to " + dlcQueueName + " failed.", e);
        } finally {
            tx.close();
        }

    }


    // added
    public void updateDLCMessageCount() {

        String dlcMessageCount = asString(brokerStore.get(bytes(FileStoreConstants.DLC_MESSAGE_COUNT)));

        if (dlcMessageCount == null) {
            dlcMessageCount = "0";
        }

        this.dlcMessageCount = Integer.parseInt(dlcMessageCount);

        this.dlcMessageCount++;

        brokerStore.put(bytes(FileStoreConstants.DLC_MESSAGE_COUNT), bytes(Long.toString(this.dlcMessageCount)));
    }

    @Override //(Implemented)
    public void moveMetadataToDLC(List<AndesMessageMetadata> messages, String dlcQueueName) throws AndesException {

        for (AndesMessageMetadata message : messages) {
            moveMetadataToDLC(message.getMessageID(), dlcQueueName);
        }

    }

    @Override //(Implemented)
    public void updateMetadataInformation(String currentQueueName, List<AndesMessageMetadata> metadataList) throws AndesException {

        Transaction tx = new Transaction(brokerStore);

        try {

            for (AndesMessageMetadata metadata : metadataList) {

                long messageID = metadata.getMessageID();
                String storageQueueName = metadata.getStorageQueueName();
                byte[] byteMetaData = metadata.getMetadata();

                //adding new metadata
                String messageMetaDataIdentifier = generateKey(FileStoreConstants.MESSAGE_METADA, FileStoreConstants.MESSAGE, Long.toString(messageID));
                tx.put(bytes(messageMetaDataIdentifier), byteMetaData);

                //changing the queue
                updateMetaDdataInfoInQueue(metadata, currentQueueName, tx);
            }

            tx.commit(brokerStore);

        } catch (DBException e) {
            tx.close();
            log.error("Updating metadata of message list failed", e);

        } finally {
            tx.close();
        }

    }

    //added
    public void updateMetaDdataInfoInQueue(AndesMessageMetadata metadata, String currentQueueName, Transaction tx) throws AndesException {

        Long messageId = metadata.getMessageID();
        String targetQueueName = metadata.getStorageQueueName();

        //updating queue_id in message metadata
        String messageQueueIDIdentifier = generateKey(FileStoreConstants.QUEUE_ID, FileStoreConstants.MESSAGE, Long.toString(messageId));
        tx.put(bytes(messageQueueIDIdentifier), bytes(getQueueID(targetQueueName)));

        // delete message from current queue
        deleteMessageFromQueue(messageId, currentQueueName, tx);

        // add message to the target queue
        createQueueMetaDataMapping(metadata, targetQueueName, tx);
    }

    @Override //(Implemented)
    public AndesMessageMetadata getMetadata(long messageId) throws AndesException {

        String messageMetaDataIdentifier = generateKey(FileStoreConstants.MESSAGE_METADA, FileStoreConstants.MESSAGE, Long.toString(messageId));
        byte[] byteMetaData = brokerStore.get(bytes(messageMetaDataIdentifier));

        return new AndesMessageMetadata(messageId, byteMetaData, true);
    }

    @Override //(Implemented)
    public List<DeliverableAndesMetadata> getMetadataList(String storageQueueName, long firstMsgId, long limit)
            throws AndesException {

        List<DeliverableAndesMetadata> metadataList = new ArrayList<>();

        DBIterator keyIterator = brokerStore.iterator();
        String head = generateKey(FileStoreConstants.MESSAGE_METADA, FileStoreConstants.QUEUE, storageQueueName, Long.toString(firstMsgId));
        keyIterator.seek(bytes(head)); // move the head to the firstMsgID

        Long messageID = firstMsgId;

        try {

            while (keyIterator.hasNext()) {

                String key = asString(keyIterator.peekNext().getKey());

                String[] keySplit = key.split(FileStoreConstants.CONNECTOR);

                if (!keySplit[keySplit.length - 1].equals(FileStoreConstants.MESSAGE_METADA)) {
                    break;
                }

                messageID = Long.parseLong(keySplit[2]); // key format : QUEUE.$queue_name.message_id. MESSAGE_METADATA : message_metadata

                byte[] byteMetadata = brokerStore.get(bytes(key));

                DeliverableAndesMetadata metadata = new DeliverableAndesMetadata(messageID, byteMetadata, true);
                metadata.setStorageQueueName(storageQueueName);

                metadataList.add(metadata);

                if (metadataList.size() > limit) {
                    break;
                }

                keyIterator.next();

            }

        } finally {

            try {

                keyIterator.close();

            } catch (IOException e) {

                log.error("Key iterator closing failed.", e);

            }

        }

        return metadataList;
    }

    @Override //(Implemented)
    public long getMessageCountForQueueInRange(String storageQueueName, long firstMessageId, long lastMessageId) throws AndesException {

        String head = generateKey(FileStoreConstants.MESSAGE_METADA, FileStoreConstants.QUEUE, storageQueueName, Long.toString(firstMessageId));
        DBIterator keyIterator = brokerStore.iterator();
        keyIterator.seek(bytes(head));

        long count = 0;

        try {

            while (keyIterator.hasNext()) {

                count++;

                String key = asString(keyIterator.peekNext().getKey());
                long messageId = Long.parseLong(key.split(FileStoreConstants.CONNECTOR)[2]);

                if (messageId >= lastMessageId) {
                    break;
                }

                keyIterator.next();
            }

        } finally {

            try {

                keyIterator.close();

            } catch (IOException e) {

                log.error("Key iterator closing failed.", e);

            }
        }

        return count;
    }

    @Override //same as getMetadataList(String storageQueueName, long firstMsgId, int limit)
    public List<AndesMessageMetadata> getMetadataList(String storageQueueName, long firstMsgId, int count) throws AndesException {
        return getMetadataList(storageQueueName, firstMsgId, count);
    }

    @Override //(Implemented)
    public List<AndesMessageMetadata> getNextNMessageMetadataForQueueFromDLC(String storageQueueName, String dlcQueueName, long firstMsgId, int count) throws AndesException { //TODO first message is missing

        List<AndesMessageMetadata> metadataList = new ArrayList<>(count);

        String head = generateKey(FileStoreConstants.MESSAGE_METADA, FileStoreConstants.QUEUE, dlcQueueName, Long.toString(firstMsgId));
        DBIterator keyIterator = brokerStore.iterator();
        keyIterator.seek(bytes(head));

        try {

            while (keyIterator.hasNext()) {

                String key = asString(keyIterator.peekNext().getKey());
                String[] keySplit = key.split(FileStoreConstants.CONNECTOR);

                if (!keySplit[keySplit.length - 1].equals(FileStoreConstants.MESSAGE_METADA)) {
                    break;
                }

                if (metadataList.size() >= count) {
                    break;
                }

                String messageID = keySplit[2];
                String messageQueueIDIdentifier = generateKey(FileStoreConstants.QUEUE_ID, FileStoreConstants.MESSAGE, messageID);
                String storageQueueID = asString(brokerStore.get(bytes(messageQueueIDIdentifier)));

                if (storageQueueID.equals(getQueueID(storageQueueName))) {

                    byte[] byteMetadata = brokerStore.get(bytes(key));
                    DeliverableAndesMetadata metadata = new DeliverableAndesMetadata(Long.parseLong(messageID), byteMetadata, true);
                    metadata.setStorageQueueName(storageQueueName);
                    metadataList.add(metadata);

                }

                keyIterator.next();
            }

        } finally {

            try {

                keyIterator.close();

            } catch (IOException e) {

                log.error("Key iterator closing failed.", e);

            }

        }

        return metadataList;
    }

    @Override //(Implemented)
    public List<AndesMessageMetadata> getNextNMessageMetadataFromDLC(String dlcQueueName, long firstMsgId, int count) throws AndesException {

        List<AndesMessageMetadata> metadataList = new ArrayList<>(count);

        String head = generateKey(FileStoreConstants.MESSAGE_METADA, FileStoreConstants.QUEUE, dlcQueueName, Long.toString(firstMsgId));
        DBIterator keyIterator = brokerStore.iterator();
        keyIterator.seek(bytes(head));

        try {

            while (keyIterator.hasNext()) {

                String key = asString(keyIterator.peekNext().getKey());
                String[] keySplit = key.split(FileStoreConstants.CONNECTOR);

                if (!keySplit[keySplit.length - 1].equals(FileStoreConstants.MESSAGE_METADA)) {
                    break;
                }

                if (metadataList.size() >= count) {
                    break;
                }

                String messageID = keySplit[2];
                byte[] byteMetadata = brokerStore.get(bytes(key));

                DeliverableAndesMetadata metadata = new DeliverableAndesMetadata(Long.parseLong(messageID), byteMetadata, true);
                metadata.setStorageQueueName(dlcQueueName);
                metadataList.add(metadata);

                keyIterator.next();
            }

        } finally {

            try {

                keyIterator.close();

            } catch (IOException e) {

                log.error("Key iterator closing failed.", e);

            }

        }

        return metadataList;
    }

    @Override //(Implemented)
    public void deleteMessageMetadataFromQueue(String storageQueueName, List<AndesMessageMetadata> messagesToRemove) throws AndesException {

        Transaction tx = new Transaction(brokerStore);

        for (AndesMessageMetadata metadata : messagesToRemove) {

            long messageID = metadata.getMessageID();

            try {
                deleteMessageMetaData(messageID, tx);
                deleteMessageFromQueue(messageID, storageQueueName, tx);

                tx.commit(brokerStore);

            } catch (DBException e) {

                tx.close();
                log.warn("Message metadata deletion from " + storageQueueName + " failed", e);

            } finally {
                tx.close();
            }


        }
    }

    @Override //(Implemented)
    public void deleteMessages(Collection<? extends AndesMessageMetadata> messagesToRemove) throws AndesException {


        for (AndesMessageMetadata metadata : messagesToRemove) {

            long messageID = metadata.getMessageID();
            String storageQueueName = metadata.getStorageQueueName();

            Transaction tx = new Transaction(brokerStore);

            try {

                deleteMessageMetaData(messageID, tx);
                deleteMessageFromQueue(messageID, storageQueueName, tx);

                // delete message from DLC if it in DLC
                String messageDLCQueueIdIdentifier = generateKey(FileStoreConstants.DLC_QUEUE_ID, FileStoreConstants.MESSAGE, Long.toString(messageID));

                if (Long.parseLong(asString(brokerStore.get(bytes(messageDLCQueueIdIdentifier)))) != -1) {
                    deleteMessageFromQueue(messageID, FileStoreConstants.DLC, tx);
                }

                tx.commit(brokerStore);

            } catch (DBException e) {

                tx.close();
                log.warn("Messages deletion failed", e);

            } finally {
                tx.close();
            }

        }

    }

    @Override //(Implemented)
    public void deleteMessages(List<Long> messagesToRemove) throws AndesException {

        for (Long messageID : messagesToRemove) {

            Transaction tx = new Transaction(brokerStore);

            try {

                //delete message metadata
                deleteMessageMetaData(messageID, tx);

                // delete message from queue
                String messageQueueIdIdentifier = generateKey(FileStoreConstants.QUEUE_ID, FileStoreConstants.MESSAGE, Long.toString(messageID));
                String storageQueueName = getQueueName(Long.parseLong(asString(brokerStore.get(bytes(messageQueueIdIdentifier)))));

                deleteMessageFromQueue(messageID, storageQueueName, tx);

                // delete message from DLC if it in DLC
                String messageDLCQueueIdIdentifier = generateKey(FileStoreConstants.DLC_QUEUE_ID, FileStoreConstants.MESSAGE, Long.toString(messageID));

                if (Long.parseLong(asString(brokerStore.get(bytes(messageDLCQueueIdIdentifier)))) != -1) {
                    deleteMessageFromQueue(messageID, FileStoreConstants.DLC, tx);
                }

                tx.commit(brokerStore);

            } catch (Exception e) {
                tx.close();
                log.warn("Messages deletion failed", e);

            } finally {
                tx.close();
            }
        }
    }

    //added
    public void deleteMessageMetaData(long messageID, Transaction tx) {


        DBIterator keyIterator = brokerStore.iterator();
        String head = generateKey(Long.toString(messageID), FileStoreConstants.MESSAGE);
        keyIterator.seek(bytes(head));

        try {

            while (keyIterator.hasNext()) {

                String key = asString(keyIterator.peekNext().getKey());
                Long currentID = Long.parseLong(key.split(FileStoreConstants.CONNECTOR)[1]);

                if (currentID != messageID) {
                    break;
                }

                tx.delete(bytes(key));

                keyIterator.next();

            }
        } finally {

            try {
                keyIterator.close();

            } catch (IOException e) {
                log.error("Key iterator closing failed.", e);

            }
        }

    }

    //added
    public void deleteMessageFromQueue(long messageID, String storageQueueName, Transaction tx) {

        String queueMessageMetaDataIdentifier = generateKey(FileStoreConstants.MESSAGE_METADA, FileStoreConstants.QUEUE, storageQueueName, Long.toString(messageID));
        String queueMessageCountIdentifier = generateKey(FileStoreConstants.MESSAGE_COUNT, FileStoreConstants.QUEUE, storageQueueName);

        Long queueMessageCount;

        if (asString(tx.getKey(queueMessageCountIdentifier)) == null) {
            queueMessageCount = Long.parseLong(asString(brokerStore.get(bytes(queueMessageCountIdentifier))));
        } else {
            queueMessageCount = Long.parseLong(asString(tx.getKey(queueMessageCountIdentifier)));
        }

        queueMessageCount--;

        tx.setKey(queueMessageCountIdentifier, bytes(Long.toString(queueMessageCount)));
        tx.delete(bytes(queueMessageMetaDataIdentifier));

    }

    @Override
    public void deleteDLCMessages(List<AndesMessageMetadata> messagesToRemove) throws AndesException {

        for (AndesMessageMetadata metadata : messagesToRemove) {

            long messageID = metadata.getMessageID();

            Transaction tx = new Transaction(brokerStore);

            try {
                deleteMessageMetaData(messageID, tx);
                deleteMessageFromQueue(messageID, FileStoreConstants.DLC, tx);

                tx.commit(brokerStore);

            } catch (DBException e) {
                tx.close();
                log.warn("Messages deletion from DLC failed", e);

            } finally {
                tx.close();
            }
        }

    }

    @Override
    public List<Long> getExpiredMessages(long lowerBoundMessageID, String queueName) throws AndesException {
        return null;
    }

    @Override
    public List<Long> getExpiredMessagesFromDLC(long messageCount) throws AndesException {
        return null;
    }

    @Override
    public void addMessageToExpiryQueue(Long messageId, Long expirationTime, boolean isMessageForTopic, String
            destination) throws AndesException {

    }

    @Override //(Implemented)
    public int deleteAllMessageMetadata(String storageQueueName) throws AndesException {

        int count = 0;

        Transaction tx = new Transaction(brokerStore);

        DBIterator keyIterator = brokerStore.iterator();
        String head = generateKey(storageQueueName, FileStoreConstants.QUEUE);
        keyIterator.seek(bytes(head));

        try {

            try {

                while (keyIterator.hasNext()) {

                    String key = asString(keyIterator.peekNext().getKey());
                    String[] keySplit = key.split(FileStoreConstants.CONNECTOR);

                    if (!keySplit[keySplit.length - 1].equals(FileStoreConstants.MESSAGE_METADA)) {
                        break;
                    }

                    long messageID = Long.parseLong(keySplit[2]);

                    deleteMessageMetaData(messageID, tx);
                    deleteMessageFromQueue(messageID, storageQueueName, tx);

                    count++;

                    keyIterator.next();

                }

            } finally {

                try {

                    keyIterator.close();

                } catch (IOException e) {

                    log.error("Key iterator closing failed.", e);

                }
            }

            tx.commit(brokerStore);

        } catch (DBException e) {
            tx.close();
            log.warn("Messages deletion from " + storageQueueName + " failed", e);

        } finally {
            tx.close();
        }

        return count;
    }

    @Override //(Implemented)
    public int clearDLCQueue(String dlcQueueName) throws AndesException {
        return deleteAllMessageMetadata(dlcQueueName);
    }

    @Override //(Implemented)
    public LongArrayList getMessageIDsAddressedToQueue(String storageQueueName, Long startMessageID) throws AndesException {

        LongArrayList messageIDs = new LongArrayList();

        DBIterator keyIterator = brokerStore.iterator();
        String head = generateKey(Long.toString(startMessageID), FileStoreConstants.QUEUE, storageQueueName);
        keyIterator.seek(bytes(head));

        try {

            while (keyIterator.hasNext()) {

                String key = asString(keyIterator.peekNext().getKey());
                String[] keySplit = key.split(FileStoreConstants.CONNECTOR);

                if (!keySplit[keySplit.length - 1].equals(FileStoreConstants.MESSAGE_METADA)) {
                    break;
                }

                Long messageID = Long.parseLong(keySplit[2]);
                messageIDs.add(messageID);

                keyIterator.next();
            }

        } finally {

            try {

                keyIterator.close();

            } catch (IOException e) {

                log.error("Key iterator closing failed.", e);

            }
        }

        return messageIDs;

    }

    @Override //(Implemented)
    public void addQueue(String storageQueueName) throws AndesException {

        updateLastQueueID();

        String queueIDIdentifier = generateKey(FileStoreConstants.QUEUE_ID, FileStoreConstants.QUEUE, storageQueueName);
        String queueNameIdentifier = generateKey(FileStoreConstants.QUEUE_NAME, FileStoreConstants.QUEUE, Integer.toString(this.lastQueueID));
        String queueMessageCountIdentifier = generateKey(FileStoreConstants.MESSAGE_COUNT, FileStoreConstants.QUEUE, storageQueueName);

        Transaction tx = new Transaction(brokerStore);

        try {

            tx.put(bytes(queueIDIdentifier), bytes(Integer.toString(this.lastQueueID)));
            tx.put(bytes(queueNameIdentifier), bytes(storageQueueName));
            tx.put(bytes(queueMessageCountIdentifier), bytes("0"));

            this.lastQueueID++;

            tx.put(bytes(FileStoreConstants.LAST_QUEUE_ID), bytes(Integer.toString(this.lastQueueID)));

            tx.commit(brokerStore);

        } catch (DBException e) {
            tx.close();
            log.error("Queue : " + storageQueueName + " adding failed", e);
        } finally {
            tx.close();
        }
    }

    @Override
    public Map<String, Integer> getMessageCountForAllQueues(List<String> queueNames) throws AndesException {

        Map<String, Integer> queueMessageCount = new HashMap<>();

        for (String queueName : queueNames) {

            String queueMessageCountIdentifier = generateKey(FileStoreConstants.MESSAGE_COUNT, FileStoreConstants.QUEUE, queueName);
            int messageCount = Integer.parseInt(asString(brokerStore.get(bytes(queueMessageCountIdentifier))));
            queueMessageCount.put(queueName, messageCount);
        }

        return queueMessageCount;
    }

    @Override
    public long getMessageCountForQueue(String storageQueueName) throws AndesException {
        String queueMessageCountIdentifier = generateKey(FileStoreConstants.MESSAGE_COUNT, FileStoreConstants.QUEUE, storageQueueName);
        long messageCount = Long.parseLong(asString(brokerStore.get(bytes(queueMessageCountIdentifier))));
        return messageCount;
    }

    @Override
    public long getApproximateQueueMessageCount(String storageQueueName) throws AndesException {
        return 0;
    }

    @Override
    public long getMessageCountForQueueInDLC(String storageQueueName, String dlcQueueName) throws AndesException {

        long count = 0;

        DBIterator keyIterator = brokerStore.iterator();
        String head = generateKey(dlcQueueName, FileStoreConstants.QUEUE);
        keyIterator.seek(bytes(head));

        try {

            while (keyIterator.hasNext()) {

                String key = asString(keyIterator.peekNext().getKey());
                String[] keySplit = key.split(FileStoreConstants.CONNECTOR);

                if (!keySplit[keySplit.length - 1].equals(FileStoreConstants.MESSAGE_METADA)) {
                    break;
                }

                String messageID = keySplit[2];
                String messageQueueIDIdentifier = generateKey(FileStoreConstants.QUEUE_ID, FileStoreConstants.MESSAGE, messageID);
                String queueID = asString(brokerStore.get(bytes(messageQueueIDIdentifier)));

                if (getQueueID(storageQueueName).equals(queueID)) {
                    count++;
                }

                keyIterator.next();
            }

        } finally {
            try {

                keyIterator.close();

            } catch (IOException e) {

                log.error("Key iterator closing failed.", e);

            }
        }

        return count;
    }

    @Override
    public long getMessageCountForDLCQueue(String dlcQueueName) throws AndesException {
        return this.dlcMessageCount;
    }

    @Override
    public void resetMessageCounterForQueue(String storageQueueName) throws AndesException {

    }

    @Override
    public void removeQueue(String storageQueueName) throws AndesException {

        Transaction tx = new Transaction(brokerStore);

        try {

            deleteAllMessagesFromQueue(storageQueueName, tx);
            deleteQueueData(storageQueueName, tx);

            tx.commit(brokerStore);

        } catch (DBException e) {

            tx.close();
            log.warn("Removing Queue : " + storageQueueName + " failed", e);

        } finally {

            tx.close();
        }

    }

    //added

    public void deleteAllMessagesFromQueue(String storageQueueName, Transaction tx) {

        DBIterator keyIterator = brokerStore.iterator();
        String head = generateKey(storageQueueName, FileStoreConstants.QUEUE);
        keyIterator.seek(bytes(head));

        try {

            while (keyIterator.hasNext()) {

                String key = asString(keyIterator.peekNext().getKey());
                String[] keySplit = key.split(FileStoreConstants.CONNECTOR);

                if (!keySplit[keySplit.length - 1].equals(FileStoreConstants.MESSAGE_METADA)) {
                    break;
                }

                long messageID = Long.parseLong(keySplit[2]);

                deleteMessageMetaData(messageID, tx);
                deleteMessageFromQueue(messageID, storageQueueName, tx);

                keyIterator.next();

            }

        } finally {

            try {
                keyIterator.close();
            } catch (IOException e) {
                log.error("Error occured while closing ", e);
            }
        }
    }

    //added

    public void deleteQueueData(String storageQueueName, Transaction tx) throws AndesException {

        String queueNameIdentifier = generateKey(FileStoreConstants.QUEUE_NAME, FileStoreConstants.QUEUE, getQueueID(storageQueueName));
        String queueIDIdentifer = generateKey(FileStoreConstants.QUEUE_ID, FileStoreConstants.QUEUE, storageQueueName);
        String queueMessageCountIdentifier = generateKey(FileStoreConstants.MESSAGE_COUNT, FileStoreConstants.QUEUE, storageQueueName);

        tx.delete(bytes(queueNameIdentifier));
        tx.delete(bytes(queueIDIdentifer));
        tx.delete(bytes(queueMessageCountIdentifier));

        this.lastQueueID--;
        tx.put(bytes(FileStoreConstants.LAST_QUEUE_ID), bytes(Long.toString(this.lastQueueID)));
    }

    @Override
    public void removeLocalQueueData(String storageQueueName) {
        // no cache is used
    }

    @Override
    public void incrementMessageCountForQueue(String storageQueueName, long incrementBy) throws AndesException {

    }

    @Override
    public void decrementMessageCountForQueue(String storageQueueName, long decrementBy) throws AndesException {

    }

    @Override
    public void storeRetainedMessages(Map<String, AndesMessage> retainMap) throws AndesException {

    }

    @Override
    public List<String> getAllRetainedTopics() throws AndesException {
        return null;
    }

    @Override
    public Map<Integer, AndesMessagePart> getRetainedContentParts(long messageID) throws AndesException {
        return null;
    }

    @Override
    public DeliverableAndesMetadata getRetainedMetadata(String destination) throws AndesException {
        return null;
    }

    @Override
    public List<Long> getMessageIdsInDLCForQueue(String sourceQueueName, String dlcQueueName, long startMessageId,
                                                 int messageLimit) throws AndesException {
        return null;
    }

    @Override
    public List<Long> getMessageIdsInDLC(String dlcQueueName, long startMessageId, int messageLimit) throws
            AndesException {
        return null;
    }

    @Override
    public void close() {
        try {
            brokerStore.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public DtxStore getDtxStore() {
        //Need to call this
        return new DtxStore() {
            @Override
            public long storeDtxRecords(Xid xid, List<AndesMessage> enqueueRecords, List<? extends
                    AndesMessageMetadata> dequeueRecords) throws AndesException {
                return 0;
            }

            @Override
            public void updateOnCommit(long internalXid, List<AndesMessage> enqueueRecords) throws AndesException {

            }

            @Override
            public void updateOnRollback(long internalXid, List<AndesPreparedMessageMetadata> messagesToRestore)
                    throws AndesException {

            }

            @Override
            public long recoverBranchData(DtxBranch branch, String nodeId) throws AndesException {
                return 0;
            }

            @Override
            public Set<XidImpl> getStoredXidSet(String nodeId) throws AndesException {
                return new HashSet<>();
            }

            @Override
            public boolean isOperational(String testString, long testTime) {
                return false;
            }
        };
    }

    //added
    public static String generateKey(String suffix, String prefix, String... identifiers) {


        for (String identifier : identifiers) {
            prefix = prefix + FileStoreConstants.CONNECTOR + identifier; // TODO change "." to ":" or "#" which are not used in AMQP queue naming
        }

        return prefix + FileStoreConstants.CONNECTOR + suffix;
    }
}
