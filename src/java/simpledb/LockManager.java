package simpledb;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by mohit on 1/10/16.
 */
public class LockManager {
    HashMap<PageId, Set<TransactionId>> readOnly;
    HashMap<PageId, TransactionId> writeOnly;
    HashMap<TransactionId, Set<PageId>> sharedPages;
    HashMap<TransactionId, Set<PageId>> exclusivePages;

    public LockManager(){
        readOnly = new HashMap<>();
        writeOnly = new HashMap<>();
        sharedPages = new HashMap<>();
        exclusivePages = new HashMap<>();
    }

    public boolean holdsLock(TransactionId tid, PageId pid){
        return readOnly.get(pid).contains(tid) || writeOnly.get(pid) == tid;
    }

    public synchronized void releaseLock(TransactionId tid, PageId pageId){
        Set<TransactionId> tids = readOnly.get(pageId);
        Set<PageId> shardPages = sharedPages.get(tid);
        Set<PageId> excluPages = exclusivePages.get(tid);

        if(tids != null && tids.contains(tid)){
            tids.remove(tid);
            readOnly.put(pageId, tids);
        }

        writeOnly.remove(pageId);

        if(shardPages!=null && shardPages.contains(pageId)){
            shardPages.remove(pageId);
            sharedPages.put(tid, shardPages);
        }

        if(excluPages!=null && excluPages.contains(pageId)){
            excluPages.remove(pageId);
            exclusivePages.put(tid, excluPages);
        }

    }

    public void releaseAllLocks(TransactionId tid){
        exclusivePages.remove(tid);
        sharedPages.remove(tid);

        for(PageId pageId : readOnly.keySet()){
            Set<TransactionId> transactionIds = readOnly.get(pageId);
            if(transactionIds != null && transactionIds.contains(tid)){
                transactionIds.remove(tid);
            }
            readOnly.put(pageId, transactionIds);
        }

        for(PageId pageId : writeOnly.keySet()){
            if(writeOnly.get(pageId) == tid){
                writeOnly.remove(pageId);
            }
        }
    }

    public synchronized boolean grantLock(TransactionId tid, PageId pageId, Permissions pm){
        if(pm == Permissions.READ_ONLY){
            Set<TransactionId> transactionIds = readOnly.get(pageId);
            TransactionId writeTid = writeOnly.get(pageId);
            if(writeTid == null || writeTid == tid){
                if(transactionIds == null){
                    transactionIds = new HashSet<>();
                }
                transactionIds.add(tid);
                readOnly.put(pageId, transactionIds);

                Set<PageId> shardPages = sharedPages.get(tid);
                if(shardPages == null){
                    shardPages = new HashSet<>();
                }
                shardPages.add(pageId);
                sharedPages.put(tid, shardPages);
                return false;
            }else{
                return true;
            }
        }else{
            Set<TransactionId> transactionIds = readOnly.get(pageId);
            TransactionId transactionId = writeOnly.get(pageId);

            if(transactionIds != null && transactionIds.size() > 1){
                return true;
            }else if(transactionIds != null && transactionIds.size() == 1 && !transactionIds.contains(tid)){
                return true;
            }else if(transactionId != null && transactionId != tid){
                return true;
            }else{
                writeOnly.put(pageId, tid);
                Set<PageId> exclPages = exclusivePages.get(tid);
                if(exclPages == null){
                    exclPages = new HashSet<>();
                }
                exclPages.add(pageId);
                exclusivePages.put(tid, exclPages);
                return false;
            }
        }
    }


}
