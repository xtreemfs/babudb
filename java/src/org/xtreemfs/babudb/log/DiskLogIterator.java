/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
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
    
    public DiskLogIterator(File[] logFiles, LSN from) {

        this.from = from;
        
        try {
            
            if (logFiles != null) {
                
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
                for (LSN lsn : copy) {
                    if (last == null)
                        last = lsn;
                    else {
                        if (from != null && lsn.compareTo(from) <= 0) {
                            orderedLogList.remove(last);
                            last = lsn;
                        } else
                            break;
                    }
                }
                
                logList = orderedLogList.iterator();
                findFirstEntry();
            }
            
        } catch (LogEntryException exc) {
            throw new RuntimeException(exc);
        } catch (IOException exc) {
            throw new RuntimeException(exc);
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
        currentFile.close();
    }
    
    protected void findFirstEntry() throws IOException, LogEntryException {
        
        if (logList == null || !logList.hasNext())
            return;
        
        currentLog = logList.next();
        currentFile = new DiskLogFile(dbLogDir, currentLog);
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
        
        // if there is another log entry in the curren file, return it
        if (currentFile.hasNext())
            return currentFile.next();
        
        // otherwise, check if there is another log file; if not, return 'null'
        if (!logList.hasNext())
            return null;
        
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
