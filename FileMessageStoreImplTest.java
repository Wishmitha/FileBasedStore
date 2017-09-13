package org.wso2.broker.test;

import com.gs.collections.impl.list.mutable.primitive.LongArrayList;
import org.testng.annotations.Test;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.AndesMessagePart;
import org.wso2.andes.store.file.FileMessageStoreImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Created by wishmitha on 9/6/17.
 */
public class FileMessageStoreImplTest {

    Logger logger = Logger.getLogger(FileMessageStoreImpl.class.getName());

    FileMessageStoreImpl fileStore = new FileMessageStoreImpl();

    ArrayList<AndesMessageMetadata> testqueue = new ArrayList<>();
    ArrayList<AndesMessageMetadata> trialqueue = new ArrayList<>();
    ArrayList<AndesMessageMetadata> deadletterchannel = new ArrayList<>();

    @Test
    public void initializeMessageStore() throws AndesException {
        fileStore.initializeMessageStore(null , null);
        logger.info("LevelDB store initialized.");
    }

    @Test(priority = 1)
    public void storeMessages() throws AndesException {
        List<AndesMessage> messagestoStore = new ArrayList<AndesMessage>();

        for (int i = 0; i < 20; i++) {
            AndesMessage message = this.createMessage("testqueue" , false);
            long messageID = System.currentTimeMillis() + i;
            message.getMetadata().setMessageID(messageID);
            message.getContentChunkList().get(0).setMessageID(messageID);
            messagestoStore.add(message);

            testqueue.add(message.getMetadata());
            logger.info("Message " + messageID + " stored in testqueue.");
        }

        fileStore.storeMessages(messagestoStore);
    }

/*    @Test(priority = 1)
    public void storeMessageRollBack() throws AndesException {
        List<AndesMessage> messagestoStore = new ArrayList<AndesMessage>();

        AndesMessage message = this.createMessage("testqueue" , true);
        messagestoStore.add(message);

        fileStore.storeMessages(messagestoStore);
    }

    @Test(priority = 1)
    public void storeMessagesRollBack() throws AndesException {
        List<AndesMessage> messagestoStore = new ArrayList<AndesMessage>();

        for (int i = 0; i < 9; i++) {
            AndesMessage message = this.createMessage("testqueue" , false);
        }

        AndesMessage message = this.createMessage("testqueue" , true);
        messagestoStore.add(message);

        fileStore.storeMessages(messagestoStore);
    }*/

    @Test(priority = 1)
    public void addQueue() throws AndesException {
        fileStore.addQueue("trialqueue");
        logger.info("Queue trialqueue added.");
    }

    @Test(priority = 2)
    public void moveMessageToAnotherQueue() throws AndesException {

        for (int i = 0; i < 5; i++) {

            AndesMessageMetadata metadata = testqueue.get(0);

            metadata.setStorageQueueName("trialqueue");
            metadata.setDestination("trialqueue");
            trialqueue.add(metadata);
            testqueue.remove(0);

            fileStore.moveMetadataToQueue(metadata.getMessageID(), "testqueue" , "trialqueue");

            logger.info("Message " + metadata.getMessageID() + " is moved to trialqueue.");
        }
    }

    @Test(priority = 2)
    public void moveMessageToDLC() throws AndesException {
        AndesMessageMetadata metadata = testqueue.get(testqueue.size() - 1);

        deadletterchannel.add(metadata);
        testqueue.remove(testqueue.size() - 1);

        fileStore.moveMetadataToDLC(metadata.getMessageID() , "deadletterchannel");

        logger.info("Message " + metadata.getMessageID() + " is moved to DLC.");
    }

    @Test(priority = 2)
    public void moveMessagesToDLC() throws AndesException {
        List<AndesMessageMetadata> messagesToMove = new ArrayList<>();

        for (int i = 0; i < 7; i++) {
            AndesMessageMetadata metadata = testqueue.get(testqueue.size() - 1);
            messagesToMove.add(metadata);
            deadletterchannel.add(metadata);
            testqueue.remove(testqueue.size() - 1);

            logger.info("Message " + metadata.getMessageID() + " is moved to DLC.");
        }
        fileStore.moveMetadataToDLC(messagesToMove , "deadletterchannel");
    }

    @Test(priority = 3)
    public void restoreMessagesFromDLC() throws AndesException {
        List<AndesMessageMetadata> messagesToRestore = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            AndesMessageMetadata metadata = deadletterchannel.get(0);
            messagesToRestore.add(metadata);
            testqueue.add(metadata);
            deadletterchannel.remove(0);

            logger.info("Message " + metadata.getMessageID() + " is restored from DLC");
        }

        for (AndesMessageMetadata metadata : messagesToRestore) {
            fileStore.moveMetadataToQueue(metadata.getMessageID() , "deadletterchannel" ,
                    metadata.getStorageQueueName());
        }

    }

    @Test(priority = 3)
    public void updateMetadataInformation() throws AndesException {
        ArrayList<AndesMessageMetadata> messagesToUpdate = new ArrayList<>();

        for (AndesMessageMetadata metadata: testqueue.subList(0 , 3)) {
            metadata.setMetadata("Updated Message".getBytes());
            messagesToUpdate.add(metadata);
            logger.info("Message " + metadata.getMessageID() + " metadata is updated.");
        }

        fileStore.updateMetadataInformation("testqueue", messagesToUpdate);
    }

   @Test(priority = 4)
    public void deleteMessagesFromQueue() throws AndesException {
        List<AndesMessageMetadata> messagesToRemove = new ArrayList<>();

        for (int i = 0; i < 2; i++) {
            AndesMessageMetadata metadata = testqueue.get(0);
            messagesToRemove.add(metadata);
            testqueue.remove(0);
            logger.info("Message " + metadata.getMessageID() + " is removed.");
        }
        fileStore.deleteMessageMetadataFromQueue("testqueue" , messagesToRemove);
    }

    @Test(priority = 4)
    public void deleteMessages() throws AndesException {
        List<AndesMessageMetadata> messagesToRemove = new ArrayList<>();

        messagesToRemove.add(testqueue.get(0));
        logger.info("Message " + testqueue.get(0).getMessageID() + " is removed");
        testqueue.remove(0);

        messagesToRemove.add(trialqueue.get(0));
        logger.info("Message " + testqueue.get(0).getMessageID() + " is removed");
        trialqueue.remove(0);

        fileStore.deleteMessages(messagesToRemove);
    }

    @Test(priority = 4)
    public void deleteMessagesWithID() throws AndesException {
        List<Long> messagesToRemove = new ArrayList<>();

        messagesToRemove.add(testqueue.get(0).getMessageID());
        logger.info("Message " + testqueue.get(0).getMessageID() + " is removed");
        testqueue.remove(0);

        messagesToRemove.add(trialqueue.get(0).getMessageID());
        logger.info("Message " + testqueue.get(0).getMessageID() + " is removed");
        trialqueue.remove(0);

        fileStore.deleteMessages(messagesToRemove);
    }

    @Test(priority = 4)
    public void deleteMessagesFromDLC() throws AndesException {
        List<AndesMessageMetadata> messagesToRemove = new ArrayList<>();

        for (int i = 0; i < 2; i++) {
            AndesMessageMetadata metadata = deadletterchannel.get(0);
            messagesToRemove.add(metadata);
            deadletterchannel.remove(0);
            logger.info("Message " + metadata.getMessageID() + " is removed from DLC.");
        }

        fileStore.deleteDLCMessages(messagesToRemove);
    }

    @Test(priority = 5)
    public void getMessageIDsForQueue() throws AndesException {

        LongArrayList messagIDs = fileStore.getMessageIDsAddressedToQueue("testqueue" ,
                testqueue.get(0).getMessageID());

        for (int i = 0; i < messagIDs.size(); i++) {
            logger.info("Message " + messagIDs.get(i) + " is in testqueue");
        }
    }

    @Test(priority = 5)
    public void getCountsForMessagesInDLC() throws AndesException {
        long testQueueCount = fileStore.getMessageCountForQueueInDLC("testqueue" , "deadletterchannel");
        long trialQueueCount = fileStore.getMessageCountForQueueInDLC("trialqueue" , "deadletterchannel");

        logger.info("testqueue has " + testQueueCount + " messages in DLC.");
        logger.info("trialtqueue has " + trialQueueCount + " messages in DLC.");
    }

    @Test(priority = 6)
    public void removeQueue() throws AndesException {
        fileStore.removeQueue("trialqueue");
        trialqueue.clear();
        logger.info("trialqueue is removed.");
    }

    @Test(priority = 6)
    public void clearQueue() throws AndesException {
        int messageCount = fileStore.deleteAllMessageMetadata("testqueue");
        testqueue.clear();
        logger.info("testqueue is cleared. " + messageCount + " messages have been removed.");
    }

    @Test(priority = 6)
    public void clearDLC() throws AndesException {
        int messageCount = fileStore.clearDLCQueue("deadletterchannel");
        deadletterchannel.clear();
        logger.info("deadletter channel is cleared. " + messageCount + " messages have been removed.");
    }

    @Test(priority = 10)
    public void displayQueueMessageCount() {
        logger.info("testqueue : " + Integer.toString(testqueue.size()));
        logger.info("trialqueue : " + Integer.toString(trialqueue.size()));
        logger.info("deadletterchannel : " + Integer.toString(deadletterchannel.size()));
    }



    public AndesMessage createMessage(String storageQueueName, boolean isFaulty) {

        long messageID = System.currentTimeMillis();
        List<AndesMessagePart> chunkList = new ArrayList<AndesMessagePart>();

        AndesMessageMetadata metadata = new AndesMessageMetadata();
        metadata.setMessageID(messageID);
        metadata.setDestination(storageQueueName);
        metadata.setStorageQueueName(storageQueueName);

        if (!isFaulty) {
            metadata.setMetadata("New Messgage".getBytes());
        }

        AndesMessagePart messagePart = new AndesMessagePart();
        messagePart.setData(("Message " + Long.toString(messageID) + "for " + storageQueueName).getBytes());
        messagePart.setOffSet(0);
        messagePart.setMessageID(messageID);
        chunkList.add(messagePart);

        AndesMessage message = new AndesMessage(metadata);
        message.setChunkList(chunkList);

        return message;
    }
}
