/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.sandbox;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.xtreemfs.include.common.logging.Logging;

import static org.xtreemfs.babudb.sandbox.ContinuesRandomGenerator.Operation.*;

/**
 * <p>If two instances of {@link Random} are created with the same seed, 
 * and the same sequence of method calls is made for each, they
 * will generate and return identical sequences of numbers. 
 * In order to guarantee this property, particular algorithms 
 * are specified for the class {@link Random}.</p>
 * 
 * This class was written for evaluating BabuDB at realistic conditions at an network/cluster.
 * 
 * @author flangner
 *
 */

public class ContinuesRandomGenerator {
    /** Meta-operations supported by the BabuDB-replication */
    public static enum Operation { copy, delete, create };
    
    public final static int MAX_INSERTS_PER_GROUP = 10;
    public final static int MIN_INSERTS_PER_GROUP = 5;
    // MAX deletes has to be <= MIN inserts per group...
    public final static int MAX_DELETES_PER_GROUP = 5;
    public final static int MIN_DELETES_PER_GROUP = 3;
    public final static int MAX_META_OPERATIONS_AT_ONCE = 3;
    public final static int MIN_META_OPERATIONS_AT_ONCE = 1;
    public final static int MAX_INDICES = 10;
    public final static int MIN_INDICES = 2;
    public final static int MAX_KEY_LENGTH = 10;
    public final static int MIN_KEY_LENGTH = 3;
    public final static int MAX_VALUE_LENGTH = 30;
    public final static int MIN_VALUE_LENGTH = 10;
    
    public final static long MAX_SEQUENCENO = (Integer.MAX_VALUE-1);
    public final static long MIN_SEQUENCENO = ReplicationLongrunTestConfig.MIN_SEQUENCENO;
		
    private final static byte[] CHARS;
    private final static String[] dbPrefixes;
    
    // list of DBs available on that scenario
    private final List<Object[]> availableDBs = new LinkedList<Object[]>();
    
    // number of sequences between 2 meta-operations
    private static final int MAX_DELAY = 3000;
    private static final int MIN_DELAY = 1000;
    
    private final List<Long> sequences;
    private final List<List<Object[]>> DBsnapshots;
    private final Map<Integer,List<Object>> operationsScenario;
    
    private final long id;
    
    static {
        int charptr = 0;
        CHARS = new byte[10+26+26];
        for (int i = 48; i < 58; i++)
            CHARS[charptr++] = (byte)i;
        for (int i = 65; i < 91; i++)
            CHARS[charptr++] = (byte)i;
        for (int i = 97; i < 122; i++)
            CHARS[charptr++] = (byte)i;
        
        dbPrefixes = new String[5];
        dbPrefixes[0] = "db";
        dbPrefixes[1] = "test";
        dbPrefixes[2] = "CAPTIONDB";
        dbPrefixes[3] = "longNameDataBase";
        dbPrefixes[4] = "spC\"!$%&*";
    }
    
    /**
     * Generates the static meta-operations scenario.
     * 
     * @param seed - has to be the same at every BabuDB.
     * @param duration - the number of sequences to proceed.
     * @return the operations scenario.
     */
    public ContinuesRandomGenerator(long seed, long duration) {
        this.id = seed;
        this.DBsnapshots = new LinkedList<List<Object[]>>();
        this.sequences = new LinkedList<Long>();
        this.operationsScenario = new HashMap<Integer, List<Object>>();
        
        int dbNo = 0;
        Random random = new Random(seed);
        
        for (int i=1;i<=duration;i+=random.nextInt(MAX_DELAY-MIN_DELAY)+MIN_DELAY) {            
            int metaOperations = random.nextInt(MAX_META_OPERATIONS_AT_ONCE-MIN_META_OPERATIONS_AT_ONCE)+MIN_META_OPERATIONS_AT_ONCE;

            for (int y=0;y<metaOperations;y++){
                List<Object> operation;
                
                // no DBs available jet --> make a create operation
                if (availableDBs.size()<=1) {
                    operation = createOperation(random, dbNo);
                    dbNo++;
                } else {
                    Operation op = Operation.values()[random.nextInt(Operation.values().length)];
                    
                    switch (op) {
                    case create:
                        operation = createOperation(random, dbNo);
                        dbNo++;
                        break;
                    case copy:
                        operation = copyOperation(random, dbNo);
                        dbNo++;
                        break;
                    case delete:
                        operation = deleteOperation(random);
                        break;
                    default:
                        throw new UnsupportedOperationException ("for "+op.toString());
                    }
                }
                operationsScenario.put(i, operation);
                i++;
            }
            sequences.add((long) i);
            
            List<Object[]> snapshot = new LinkedList<Object[]>();
            for (Object[] db : availableDBs) 
                snapshot.add(db.clone());
            
            DBsnapshots.add(snapshot);
        }
    }
    
    public Map<Integer,List<Object>> getOperationsScenario(){
        return operationsScenario;
    }

    /**
     * <p>Keep an eye on the side-effects.</p>
     * 
     * @param random
     * @return a generated delete-meta-operation.
     */
    private List<Object> deleteOperation(Random random) {   	
    	List<Object> operation = new LinkedList<Object>();
    	
    	operation.add(delete);
    	// get a random DB
    	Object[] dbData = availableDBs.get(random.nextInt(availableDBs.size()));
    	String dbName = (String) dbData[0];
    	operation.add(dbName);
    	// update availableDBs
    	availableDBs.remove(dbData);
    	
    	return operation;
    }
    
    /**
     * <p>Keep an eye on the side-effects.</p>
     * 
     * @param random
     * @param dbNo
     * @return a generated copy-meta-operation.
     */
    private List<Object> copyOperation(Random random, int dbNo) {
    	List<Object> operation = new LinkedList<Object>();
    	
    	operation.add(copy);
    	// get source DB
    	Object[] dbData = availableDBs.get(random.nextInt(availableDBs.size()));
    	String sourceDB = (String) dbData[0];
    	int indices = (Integer) dbData[1];
    	operation.add(sourceDB); 	
    	// generate dbName
    	String dbName = dbPrefixes[random.nextInt(dbPrefixes.length)] + dbNo;
		operation.add(dbName);
		// update availableDBs
		availableDBs.add(new Object[]{dbName,indices});
		
		return operation;
    }

    /**
     * <p>Keep an eye on the side-effects.</p>
     * 
     * @param random
     * @param dbNo
     * @return a generated create-meta-operation.
     */
    private List<Object> createOperation(Random random, int dbNo) {
    	List<Object> operation = new LinkedList<Object>();
    	
    	operation.add(create);
		// generate dbName
		String dbName = dbPrefixes[random.nextInt(dbPrefixes.length)] + dbNo;
		operation.add(dbName);
		// generate indices
		int indices = random.nextInt(MAX_INDICES-MIN_INDICES)+MIN_INDICES;
		operation.add(indices);
		// update availableDBs
		availableDBs.add(new Object[]{dbName,indices});
		
		return operation;
    }
    
    /**
     * <p>Keep an eye on the side-effects.</p>
     * 
     * @param random
     * @param length
     * @return a random byte[] with given <code>length</code>.
     */
    private byte[] createRandomBytes(Random random,int length) {
        byte[] bytes = new byte[length];

        for (int i = 0; i < bytes.length; i++)
            bytes[i] = CHARS[random.nextInt(CHARS.length)];
        
        return bytes;
    }
	
    /**
     * THIS IS ONLY FOR THE MASTER TO GENERATE INPUTDATA
     * 
     * Precondition: RandomGenerator has to be initialized!
     * 
     * @param sequenceNo
     * @return a random-generated {@link InsertGroup} for directInsert into the BabuDB, or null, if the requested call was a meta-operation.
     * @throws Exception
     */
    public InsertGroup getInsertGroup(long sequenceNo) throws Exception{
		InsertGroup result;
    	
		if (sequenceNo>MAX_SEQUENCENO) throw new Exception(sequenceNo+" is a too big sequence number, randomGenerator has to be extended.");
		if (operationsScenario.get((int) sequenceNo)!=null) return null;
		// setup random with seed from LSN
		Random random = new Random(sequenceNo);
		
		// get the DB affected by the insert
		int seqIndex = sequences.size()-1;
		for (int i=0;i<=seqIndex;i++) {
		    if (sequences.get(i)>sequenceNo){
		        if (i>0) seqIndex = i-1;
		        else assert(false) : "There is no insert available for the given seqenceID: "+sequenceNo;
		        break;
		    }
		}
		
		List<Object[]> dbs = DBsnapshots.get(seqIndex);
		Object[] db = dbs.get(random.nextInt(dbs.size()));
		String dbName = (String) db[0];
		int dbIndices = (Integer) db[1];
		
		result = this.new InsertGroup(dbName);
		
		// generate some reconstructible key-value-pairs for insert
		int insertsPerGroup = random.nextInt(MAX_INSERTS_PER_GROUP-MIN_INSERTS_PER_GROUP)+MIN_INSERTS_PER_GROUP;
		
		for (int i=0;i<insertsPerGroup;i++){
			int keyLength = random.nextInt(MAX_KEY_LENGTH-MIN_KEY_LENGTH)+MIN_KEY_LENGTH;
			int valueLength = random.nextInt(MAX_VALUE_LENGTH-MIN_VALUE_LENGTH)+MIN_VALUE_LENGTH;
			int index = random.nextInt(dbIndices);
			
			result.addInsert(index, createRandomBytes(random, keyLength), createRandomBytes(random, valueLength));
		}
		
		// generates some deletes from the previous insertGroup, if it uses the same DB
		if ((sequenceNo-1L>0) && operationsScenario.get(sequenceNo-1L)==null) {
    		LookupGroup lg = getLookupGroup(sequenceNo-1L);
    		
    		// unable to remove previous inserts
    		if (lg == null || lg.dbName!=dbName) return result;
    		
    		// generates some deletes 
			int deletesPerGroup = random.nextInt(MAX_DELETES_PER_GROUP-MIN_DELETES_PER_GROUP)+MIN_DELETES_PER_GROUP;
			int nOInserts = lg.values.size();
			assert (nOInserts>=deletesPerGroup) : "Too many deletes for not enough inserts.";
			
			List<Integer> insertIndices = new LinkedList<Integer>();
			for (int i=0;i<nOInserts;i++)
			    insertIndices.add(i);
			
			for (int i=0;i<deletesPerGroup;i++){
				int index = insertIndices.remove(random.nextInt(insertIndices.size()));
				
				result.addDelete(lg.getIndex(index), lg.getKey(index));				
			}
		}	
		return result;
    }
    
    /**
     * THIS IS FOR THE SLAVES TO CHECK CONSISTENCY
     * 
     * Precondition: RandomGenerator has to be initialized! 
     * 
     * @param seqNo
     * @return restores a random GroupInsert for looking it up as a {@link LookupGroup} with directLookup at the BabuDB, or null if the requested call was a meta-operation.
     * @throws Exception
     */
    public LookupGroup getLookupGroup(long seqNo) throws Exception {
    	LookupGroup result;
    	
		if (seqNo>MAX_SEQUENCENO) throw new Exception(seqNo+" is a too big sequence number, randomGenerator has to be extended.");
		if (operationsScenario.get((int) seqNo)!=null) return null;
		// setup random with seed from LSN
		Random random = new Random(seqNo);
		
        // get the DB affected by the insert
        int seqIndex = sequences.size()-1;
        for (int i=0;i<=seqIndex;i++) {
            if (sequences.get(i)>seqNo){
                if (i>0) seqIndex = i-1;
                else assert(false) : "There is no insert available for the given seqenceID: "+seqNo+"\n"+toString();
                break;
            }
        }
        
        List<Object[]> dbs = DBsnapshots.get(seqIndex);
		Object[] db = dbs.get(random.nextInt(dbs.size()));
		String dbName = (String) db[0];
		int dbIndices = (Integer) db[1];
		
		result = this.new LookupGroup(dbName);
		
		// generate some reconstructable key-value-pairs for insert
		int insertsPerGroup = random.nextInt(MAX_INSERTS_PER_GROUP-MIN_INSERTS_PER_GROUP)+MIN_INSERTS_PER_GROUP;
		
		for (int i=0;i<insertsPerGroup;i++){
			int keyLength = random.nextInt(MAX_KEY_LENGTH-MIN_KEY_LENGTH)+MIN_KEY_LENGTH;
			int valueLength = random.nextInt(MAX_VALUE_LENGTH-MIN_VALUE_LENGTH)+MIN_VALUE_LENGTH;
			int index = random.nextInt(dbIndices);
			
			result.addInsert(index, createRandomBytes(random, keyLength), createRandomBytes(random, valueLength));
		}
		
		return result;
    }
    
    /**
     * @return a new random seed for initialization
     */
    public static long getRandomSeed(){
    	return new Random().nextLong();
    }
    
    public class InsertGroup {
    	public final String dbName;
    	private final List<Integer> indices = new LinkedList<Integer>();
    	private final List<byte[]> keys = new LinkedList<byte[]>();
    	private final List<byte[]> values = new LinkedList<byte[]>();
    	
    	public InsertGroup(String dbName) {
			this.dbName = dbName;
		}
    	/**
    	 * Do not insert anything, after you performed addDelete!
    	 * @param indexId
    	 * @param key
    	 * @param value
    	 */
    	public void addInsert(int indexId, byte[] key, byte[] value){
    		indices.add(indexId);
    		keys.add(key);
    		values.add(value);
    	}
    	
    	public void addDelete(int indexId, byte[] key){
    		indices.add(indexId);
    		keys.add(key);
    	}
    	
    	public byte[] getKey(int i){
    		return keys.get(i);
    	}
    	
    	public byte[] getValue(int i){
    		return values.get(i);
    	}
    	
    	public int getIndex(int i){
    		return indices.get(i);
    	}
    	
    	public int getNoInserts(){
    		return values.size();
    	}
    	
    	public int size() {
    		return keys.size();
    	}
    	
    	@Override
    	public String toString() {
    		String string = "InsertGroupData-------------------\n";
    		string += "for DB: "+dbName+"\n";
    		string += "Index | Key              | Value\n";
    		for (int i=0;i<size();i++) {
    			int index = indices.get(i);
    			string += index+((index<10) ? "     | " : "    | ");
    			String key = new String(keys.get(i));
    			string += key;
    			for (int y=key.length();y<17;y++)
    				string += " ";
    			
    			string += "| ";
    			if (values.size() <= i) string += "null (delete)\n";
    			else string += new String(values.get(i))+"\n";
    		}
    		return string;
    	}
    	
    	public String lookUpCompareable(){
    		String string = "LookupGroupData-------------------\n";
    		string += "for DB: "+dbName+"\n";
    		string += "Index | Key              | Value\n";
    		for (int i=0;i<getNoInserts();i++) {
    			int index = indices.get(i);
    			string += index+((index<10) ? "     | " : "    | ");
    			String key = new String(keys.get(i));
    			string += key;
    			for (int y=key.length();y<17;y++)
    				string += " ";
    			
    			string += "| "+new String(values.get(i))+"\n";
    		}
    		return string;
    	}
    }
    
    public class LookupGroup {
    	public final String dbName;
    	private final List<Integer> indices = new LinkedList<Integer>();
    	private final List<byte[]> keys = new LinkedList<byte[]>();
    	private final List<byte[]> values = new LinkedList<byte[]>();
    	
    	public LookupGroup(String dbName) {
			this.dbName = dbName;
		}
    	
    	public void addInsert(int indexId, byte[] key, byte[] value){
    		indices.add(indexId);
    		keys.add(key);
    		values.add(value);
    	}
    	
    	public byte[] getKey(int i){
    		return keys.get(i);
    	}
    	
    	public byte[] getValue(int i){
    		return values.get(i);
    	}
    	
    	public int getIndex(int i){
    		return indices.get(i);
    	}
    	
    	public int size(){
    		return values.size();
    	}
    	
    	@Override
    	public String toString() {
    		String string = "LookupGroupData-------------------\n";
    		string += "for DB: "+dbName+"\n";
    		string += "Index | Key              | Value\n";
    		for (int i=0;i<size();i++) {
    			int index = indices.get(i);
    			string += index+((index<10) ? "     | " : "    | ");
    			String key = new String(keys.get(i));
    			string += key;
    			for (int y=key.length();y<17;y++)
    				string += " ";
    			
    			string += "| "+new String(values.get(i))+"\n";
    		}
    		return string;
    	}
    }
    
    public String operationToString(long seqNo, List<Object> op) {
        String result = seqNo+" | "+((Operation) op.get(0)).toString();
        for (int i = 1;i<op.size();i++) 
            result += " | "+op.get(i);
        
        return result += "\n";
    }
    
    /*
     * (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        String string = "ContinuesRandomGenerator-"+this.id+"\n";
        
        if (Logging.isDebug()) {    
            string += "The operations scenario:\n";
            string += "SequenceNo | Operation | Parameters\n";
            string += "-----------------------------------\n";
            List<Integer> seqs = new LinkedList<Integer>(operationsScenario.keySet());
            Collections.sort(seqs);
            for (Integer seq : seqs){
                string += operationToString(seq, operationsScenario.get(seq));
            }
            string += "\n\r";
        }
        return string;
    }
}
