/*
 * Copyright (c) 2009-2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.log;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.xtreemfs.babudb.lsmdb.LSMDatabase;
import org.xtreemfs.babudb.lsmdb.LSN;

/**
 * An iterator that returns log entries from multiple log files.
 * 
 * @author stender, bjko
 */
public class DiskLogIterator implements Iterator<LogEntry> {
    
    private String        dbLogDir;
    
    private LSN           from;
    
    private Iterator<LSN> logList;
    
    private LSN           currentLog;
    
    private DiskLogFile   currentFile;
    
    private LogEntry      nextEntry;
    
    /**
     * @param logFiles
     * @param from
     *            - inclusive, if everything went fine, next() will return the
     *            log entry identified by LSN <code>from</code>.
     * @throws LogEntryException
     * @throws IOException
     */
    public DiskLogIterator(File[] logFiles, LSN from) throws LogEntryException, IOException {
        
        this.from = from;
        
        if (logFiles != null && logFiles.length > 0) {
            
            dbLogDir = logFiles[0].getParent() + "/";
            
            // read list of logs and create a list ordered from min LSN to
            // max LSN
            int count = -1;
            SortedSet<LSN> orderedLogList = new TreeSet<LSN>();
            Pattern p = Pattern.compile("(\\d+)\\.(\\d+)\\.dbl");
            for (File logFile : logFiles) {
                Matcher m = p.matcher(logFile.getName());
                m.matches();
                String tmp = m.group(1);
                int viewId = Integer.valueOf(tmp);
                tmp = m.group(2);
                int seqNo = Integer.valueOf(tmp);
                orderedLogList.add(new LSN(viewId, seqNo));
                count++;
            }
            LSN[] copy = orderedLogList.toArray(new LSN[orderedLogList.size()]);
            LSN last = null;
            LSN lastRemoved = null;
            for (LSN lsn : copy) {
                if (last == null)
                    last = lsn;
                else {
                    if (from != null && lsn.compareTo(from) <= 0) {
                        orderedLogList.remove(last);
                        lastRemoved = last;
                        last = lsn;
                    } else
                        break;
                }
            }
            
            // TODO invalid since natural gaps from viewId incrementation have to be tolerated
            // check if log entries are missing
            if (from != null && !LSMDatabase.NO_DB_LSN.equals(from) && last.getViewId() == from.getViewId()
                    && last.getSequenceNo() > from.getSequenceNo())
                throw new LogEntryException("missing log entries: database ends at LSN " + from.toString()
                        + ", first log entry LSN is " + last.toString());
            
            // re-add the last removed log file, if there is a chance, that
            // from is located there
            if (lastRemoved != null && from.compareTo(last) < 0) {
                orderedLogList.add(lastRemoved);
            }
            logList = orderedLogList.iterator();
            
            findFirstEntry();
        }
    }
    
    @Override
    public boolean hasNext() {
        return nextEntry != null;
    }
    
    @Override
    public LogEntry next() {
        
        try {
            
            if (nextEntry == null)
                throw new NoSuchElementException();
            
            LogEntry tmp = nextEntry;
            nextEntry = findNextEntry();
            return tmp;
            
        } catch (LogEntryException exc) {
            throw new RuntimeException(exc);
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }
    
    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
    
    public void destroy() throws IOException {
        LogEntry tmp = nextEntry;
        nextEntry = null;
        if (tmp != null)
            tmp.free();
        if(currentFile != null)
            currentFile.close();
    }
    
    protected void findFirstEntry() throws IOException, LogEntryException {
        
        if (logList == null || !logList.hasNext())
            return;
        
        do {
            currentLog = logList.next();
            if(currentFile != null)
                currentFile.close();
            currentFile = new DiskLogFile(dbLogDir, currentLog);
        } while (!currentFile.hasNext() && logList.hasNext());
        
        while (currentFile.hasNext()) {
            LogEntry le = currentFile.next();
            if (from == null || le.getLSN().compareTo(from) >= 0) {
                nextEntry = le;
                break;
            }
            le.free();
        }
    }
    
    protected LogEntry findNextEntry() throws IOException, LogEntryException {
        
        if (logList == null)
            return null;
        
        // if there is another log entry in the current file, return it
        if (currentFile.hasNext())
            return currentFile.next();
        
        // if all entries have been read from the current file, close it
        currentFile.close();
        
        // if there are no more log entries to read,
        if (!logList.hasNext())
            return null;
        
        // in any other case, switch to the next log file and repeat the
        // procedure
        currentLog = logList.next();
        currentFile = new DiskLogFile(dbLogDir, currentLog);
        return findNextEntry();
    }
}