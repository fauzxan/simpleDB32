package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;


/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc td;

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
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.file.getAbsoluteFile().hashCode();
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
    public Page readPage(PageId pid) throws IllegalArgumentException{
        // some code goes here
        HeapPage page = null;
        IllegalArgumentException t = new IllegalArgumentException();

        int pgNo = pid.getPageNumber();
        long pos = pgNo * BufferPool.getPageSize();
        byte[] temp_file = new byte[BufferPool.getPageSize()];

        try {
            RandomAccessFile raf = new RandomAccessFile(file.getAbsolutePath(), "r");
            long max = raf.length();
            if(pos >= max)
                throw t;
            raf.seek(pos);
            raf.read(temp_file, 0, temp_file.length);
            raf.close();
            page = new HeapPage((HeapPageId) pid, temp_file);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return page;
    }

    public void writePage(Page page) throws IOException {
        // some code goes here
        PageId pid = page.getId();
        long pos =  pid.getPageNumber() * BufferPool.getPageSize();
        byte[] data = page.getPageData();
        try {
            RandomAccessFile raf = new RandomAccessFile(this.file, "rw");
            raf.seek(pos);
            raf.write(data);
            raf.close();
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        try {
            return (int) file.length()/BufferPool.getPageSize();
        }
        catch(Exception e){
            return 0;
        }
    }

    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        HeapPage page = null;
        HeapPageId pid;

        int pg=0;
        while(pg<numPages()){
            pid = new HeapPageId(getId(), pg);
            page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            if(page.getNumEmptySlots()>0) {
                page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
                page.insertTuple(t);
                page.markDirty(true, tid);
                break;
            } else{
                Database.getBufferPool().unsafeReleasePage(tid, pid);
            }
            pg += 1;
        }

        if(pg==numPages()) {
            pid = new HeapPageId(getId(), pg);
            page = new HeapPage(pid, HeapPage.createEmptyPageData());
            page.insertTuple(t);
            page.markDirty(true, tid);
            writePage(page);
        }

        ArrayList<Page> modpages = new ArrayList<>();
        modpages.add(page);
        return modpages;
    }

    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        PageId pid = t.getRecordId().getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        if (page==null)
            throw new DbException("no page");
        page.deleteTuple(t);
        ArrayList<Page> modpages = new ArrayList<>();
        modpages.add(page);
        return modpages;
    }

    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        DbFileIterator dbfileiter = new DbFileIterator(){
            Iterator<Tuple> tup_iter;
            int pageNo = 0;
            boolean isOpen = false;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                tup_iter = ((HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), 0), Permissions.READ_ONLY)).iterator();
                isOpen = true;

            }

            @Override
            public void close() {
                // Not sure what to write here
                // page_iter = null;
                tup_iter = null;
                pageNo = -1;
                isOpen = false;

            }
            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (tup_iter == null) {
                    return false;
                }
                while (!tup_iter.hasNext()) {
                    pageNo+=1;
                    if (pageNo >= numPages()) {
                        return false;
                    }
                    tup_iter = ((HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pageNo), Permissions.READ_ONLY)).iterator();
                }
                return true;

            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                try{
                    if (tup_iter.hasNext()){
                        return tup_iter.next();
                    }
                }
                catch(Exception e){
                    throw new NoSuchElementException();
                }
                throw new NoSuchElementException();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                this.close();
                this.open();
            }
        };
        return dbfileiter;
    }
}

