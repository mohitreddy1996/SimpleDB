package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc td;
    private TransactionId tid;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        try {
            /*
             random access file. Create a data (byte) array of size = page_size.
             put the pointer to the beginning of the page. get page number using size of the page from buffer pool.
             read the data and put it in the read format of random access file.
             return the page.
             */
            RandomAccessFile pageFile = new RandomAccessFile(this.file, "r");
            byte[] pageData = new byte[BufferPool.PAGE_SIZE];
            pageFile.seek(BufferPool.PAGE_SIZE * pid.pageNumber());
            pageFile.read(pageData, 0, BufferPool.PAGE_SIZE);
            pageFile.close();
            return new HeapPage((HeapPageId) pid, pageData);
        } catch (Exception e){
            return null;
        }

    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for proj1
        try{
            PageId pid = page.getId();

            RandomAccessFile raf = new RandomAccessFile(this.file, "rw");
            raf.seek(pid.pageNumber() * BufferPool.PAGE_SIZE);
            raf.write(page.getPageData(), 0, BufferPool.PAGE_SIZE);
            raf.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) this.file.length()/BufferPool.PAGE_SIZE;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        ArrayList<Page> pages = new ArrayList<>();
        try{
            if(this.getEmptyPages(tid).isEmpty()){
                HeapPageId heapPageId = new HeapPageId(this.getId(), this.numPages());
                HeapPage heapPage = new HeapPage(heapPageId, HeapPage.createEmptyPageData());
                heapPage.insertTuple(t);
                this.writePage(heapPage);
                pages.add(heapPage);
            }else{
                Page page = this.getEmptyPages(tid).get(0);
                HeapPage heapPage = (HeapPage) page;
                heapPage.insertTuple(t);
                pages.add(heapPage);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return pages;
        // not necessary for proj1

    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        RecordId rid = t.getRecordId();
        PageId pageId = rid.getPageId();
        Page page = Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        HeapPage heapPage = (HeapPage) page;
        heapPage.deleteTuple(t);
        return Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileReaderIterator(tid, this);
    }

    public ArrayList<Page> getEmptyPages(TransactionId tid) throws DbException{
        ArrayList<Page> pages = new ArrayList<>();
        try{
            int tableId = this.getId();
            for(int i=0; i<this.numPages(); i++){
                HeapPageId hpid = new HeapPageId(tableId, i);
                Page page = Database.getBufferPool().getPage(tid, hpid, Permissions.READ_ONLY);
                if(((HeapPage)page).getNumEmptySlots()!=0){
                    pages.add(page);
                }
            }
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        }catch (Exception e){
            e.printStackTrace();
        }
        return pages;
    }

}

