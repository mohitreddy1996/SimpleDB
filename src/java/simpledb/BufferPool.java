package simpledb;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private int maxNumPages;
    private static int numberEntries = 0;
    private HashMap<PageId, Page> pageIdPageHashMap;
    private HashMap<PageId, Integer> recentlyUsed;
    private volatile LockManager lockManager;
    HashMap<TransactionId, Long> allTransactions;
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.maxNumPages = numPages;
        this.pageIdPageHashMap = new HashMap<>();
        this.recentlyUsed = new HashMap<>();
        lockManager = new LockManager();
        allTransactions = new HashMap<>();
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {

        if(!allTransactions.containsKey(tid)){
            allTransactions.put(tid, System.currentTimeMillis());
            boolean granted = lockManager.grantLock(tid, pid, perm);
            while (granted){
                if(System.currentTimeMillis() - allTransactions.get(tid) > 250){
                    throw  new TransactionAbortedException();
                }

                try{
                    Thread.sleep(200);
                    granted = lockManager.grantLock(tid, pid, perm);
                }catch (Exception e){
                    e.printStackTrace();
                }
            }

        }else{
            boolean granted = lockManager.grantLock(tid, pid, perm);
            while (granted){
                if(System.currentTimeMillis() - allTransactions.get(tid) > 500){
                    throw new TransactionAbortedException();
                }

                try{
                    Thread.sleep(10);
                    granted = lockManager.grantLock(tid, pid, perm);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }


        // some code goes here
        if(this.pageIdPageHashMap.containsKey(pid)){
            updateRecentlyUsed();
            recentlyUsed.put(pid, 0);
            return this.pageIdPageHashMap.get(pid);
        }else{

            HashMap<DbFile, String> filePrimaryKeyMap = Database.getCatalog().filePrimaryKeyMap();
            for(DbFile dbFile : filePrimaryKeyMap.keySet()){
                if(dbFile.getId() == pid.getTableId()){
                    Page page = dbFile.readPage(pid);
                    if(pageIdPageHashMap.size() >= maxNumPages){
                        evictPage();
                    }
                    this.pageIdPageHashMap.put(pid, page);
                    updateRecentlyUsed();
                    recentlyUsed.put(pid, 0);
                    return page;
                }
            }
        }
        return null;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for proj1
        lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj1
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for proj1
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for proj1
        // commit or Abort. commit : remove all dirty pages and revert back to the original page.
        // abort : revert back.  Both the cases just remove all the locks for that transaction.
        allTransactions.remove(tid);
        if(!commit){
            for(Page page : pageIdPageHashMap.values()){
                if(page.isDirty() != null && page.isDirty() == tid){
                    pageIdPageHashMap.put(page.getId(), page.getBeforeImage());
                }
            }
        }else{
            for(Page page : pageIdPageHashMap.values()){
                if(page.isDirty() != null && page.isDirty() == tid) {
                    flushPage(page.getId());
                    page.getBeforeImage();
                }

                if(page.isDirty() == null){
                    page.getBeforeImage();
                }
            }
        }
        lockManager.releaseAllLocks(tid);
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock 
     * acquisition is not needed for lab2). May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for proj1
        try{
            ArrayList<Page> pages = new ArrayList<>();
            DbFile dbFile = Database.getCatalog().getDbFile(tableId);
            HeapFile heapFile = (HeapFile) dbFile;
            pages = heapFile.insertTuple(tid, t);
            for(Page page : pages){
                pageIdPageHashMap.put(page.getId(), page);
                page.markDirty(true, tid);
            }
        }catch (Exception e){
            throw new DbException("");
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have 
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t the tuple to add
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        // some code goes here
        // not necessary for proj1
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile dbFile = Database.getCatalog().getDbFile(tableId);
        Page page = dbFile.deleteTuple(tid, t);
        page.markDirty(true, tid);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for proj1
        for (PageId key : pageIdPageHashMap.keySet()) {
            flushPage(key);
        }

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
	// not necessary for proj1
        pageIdPageHashMap.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for proj1
        int tableId = pid.getTableId();
        HeapFile heapFile = (HeapFile) Database.getCatalog().getDbFile(tableId);
        heapFile.writePage(pageIdPageHashMap.get(pid));
        pageIdPageHashMap.get(pid).markDirty(false, null);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj1
        for(Page page : pageIdPageHashMap.values()){
            if(page.isDirty() != null && page.isDirty() == tid){
                flushPage(page.getId());
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for proj1
        int numDirtyPages = 0;
        for(PageId pageId : pageIdPageHashMap.keySet()){
            HeapPage heapPage= (HeapPage) pageIdPageHashMap.get(pageId);
            if(heapPage.isDirty() != null){
                numDirtyPages++;
            }
        }

        if(numDirtyPages == maxNumPages){
            throw new DbException("");
        }

        int value = -1;
        PageId pageIdToBeRemoved = null;
        for (PageId pageId : recentlyUsed.keySet()) {
            int tempValue = recentlyUsed.get(pageId);
            if (tempValue > value) {
                value = tempValue;
                pageIdToBeRemoved = pageId;
                if (pageIdPageHashMap.get(pageId).isDirty() == null) {
                    try {
                        flushPage(pageIdToBeRemoved);
                        recentlyUsed.remove(pageIdToBeRemoved);
                        pageIdPageHashMap.remove(pageIdToBeRemoved);
                        break;
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }



    }

    private void updateRecentlyUsed(){
        if(!recentlyUsed.isEmpty()) {
            for (PageId pageId : recentlyUsed.keySet()) {
                int value = recentlyUsed.get(pageId);
                value++;
                recentlyUsed.put(pageId, value);
            }
        }
    }

}
