/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.sandbox;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.foundation.logging.Logging;
import org.xtreemfs.babudb.sandbox.ContinuesRandomGenerator.Operation;

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
@Deprecated
public class RandomGenerator {
    public final static int MAX_INSERTS_PER_GROUP = 10;
    public final static int MIN_INSERTS_PER_GROUP = 5;
    public final static int MAX_DELETES_PER_GROUP = 5;
    public final static int MIN_DELETES_PER_GROUP = 3;
    public final static int MAX_META_OPERATIONS_PER_VIEWID = 3;
    public final static int MIN_META_OPERATIONS_PER_VIEWID = 1;
    public final static int MAX_INDICES = 10;
    public final static int MIN_INDICES = 2;
    public final static int MAX_KEY_LENGTH = 10;
    public final static int MIN_KEY_LENGTH = 3;
    public final static int MAX_VALUE_LENGTH = 30;
    public final static int MIN_VALUE_LENGTH = 10;
    
    public final static int MAX_VIEWID = 29;
    public final static long MAX_SEQUENCENO;
    public final static long MIN_SEQUENCENO = ReplicationLongrunTestConfig.MIN_SEQUENCENO;
	
    // table of tables of meta operations ordered by their viewId
    public final Map<Integer,List<List<Object>>> operationsScenario = new HashMap<Integer,List<List<Object>>>();
	
    private final static byte[] CHARS;
    private final static long[] prims = new long[MAX_VIEWID];
    private final static String[] dbPrefixes;
    
    // map that shows at which viewIds a DB given by its name and number of indices exists.
    private final Map<Integer, List<Object[]>> dbExist = new HashMap<Integer, List<Object[]>>();
    private InsertGroup lastISG = null;
    
    static {
        int charptr = 0;
        CHARS = new byte[10+26+26];
        for (int i = 48; i < 58; i++)
            CHARS[charptr++] = (byte)i;
        for (int i = 65; i < 91; i++)
            CHARS[charptr++] = (byte)i;
        for (int i = 97; i < 122; i++)
            CHARS[charptr++] = (byte)i;
    
        // have to be big (>10000) and ascending ordered
        prims[0] = 3001L;
        prims[1] = 3011L;
        prims[2] = 3019L;
        prims[3] = 3023L;
        prims[4] = 3037L;
        prims[5] = 3041L;
        prims[6] = 3049L;
        prims[7] = 3061L;
        prims[8] = 3079L;
        prims[9] = 3083L;
        prims[10] = 3089L;
        prims[11] = 3109L;
        prims[12] = 3119L;  
        prims[13] = 3121L;
        prims[14] = 3137L;
        prims[15] = 3163L;
        prims[16] = 3167L;
        prims[17] = 3169L;
        prims[18] = 3181L;
        prims[19] = 3187L;
        prims[20] = 3191L;
        prims[21] = 3203L; 
        prims[20] = 3209L;
        prims[21] = 3217L;
        prims[22] = 3221L;
        prims[23] = 3229L;
        prims[24] = 3251L;
        prims[25] = 3253L;
        prims[26] = 3257L;
        prims[27] = 3259L;
        prims[28] = 3271L;
        MAX_SEQUENCENO = prims[0];
        
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
     * @return the operations scenario.
     */
    public Map<Integer,List<List<Object>>> initialize(Long seed) {
    	int dbNo = 0;
    	
    	Random random = new Random(seed);
    	
    	for (int i=1;i<=prims.length;i++){
    		List<Object[]> availableDBs = dbExist.get(i-1);
			if (availableDBs!=null)
    			dbExist.put(i, new LinkedList<Object[]>(availableDBs));
    		else availableDBs = new LinkedList<Object[]>();
    		
    		int metaOperations = random.nextInt(MAX_META_OPERATIONS_PER_VIEWID-MIN_META_OPERATIONS_PER_VIEWID)+MIN_META_OPERATIONS_PER_VIEWID;
    		List<List<Object>> opsAtView = new LinkedList<List<Object>>();
    		
    		for (int y=0;y<metaOperations;y++){
        		availableDBs = dbExist.get(i);
    			if (availableDBs==null) availableDBs = new LinkedList<Object[]>();
    			List<Object> operation;
    			
    			// no DBs available jet --> make a create operation
    			if (availableDBs.size()<=1) {
    				operation = createOperation(random, dbNo, i);
    				dbNo++;
    			} else {
    				Operation op = Operation.values()[random.nextInt(Operation.values().length)];
    				
    				switch (op) {
    				case create:
    					operation = createOperation(random, dbNo, i);
        				dbNo++;
        				break;
    				case copy:
    					operation = copyOperation(random, dbNo, i);
    					dbNo++;
    					break;
    				case delete:
    					operation = deleteOperation(random, i);
    					break;
    				default:
    					throw new UnsupportedOperationException ("for "+op.toString());
    				}
    			}
    			opsAtView.add(operation);
    		}
    		operationsScenario.put(i, opsAtView);
    	}
    	
    	Logging.logMessage(Logging.LEVEL_DEBUG, this, "%s", this.toString());
    	
    	return operationsScenario;
    }

    /**
     * <p>Keep an eye on the side-effects.</p>
     * 
     * @param random
     * @param viewId
     * @return a generated delete-meta-operation.
     */
    private List<Object> deleteOperation(Random random, int viewId) {   	
    	List<Object> operation = new LinkedList<Object>();
    	
    	operation.add(delete);
    	// get a random db
    	List<Object[]> dbs = dbExist.get(viewId);
    	Object[] dbData = dbs.get(random.nextInt(dbs.size()));
    	String dbName = (String) dbData[0];
    	int indices = (Integer) dbData[1];
    	operation.add(dbName);
    	
		// update dbExists-map
    	Object[] toDelete = null;
    	for (Object[] db : dbs){
    		if ((String) db[0] == dbName && (Integer) db[1] == indices) {
    			toDelete = db;
    			break;
    		}
    	}
    	assert (toDelete!=null) : dbName+" was not available for deletion.";
    	dbs.remove(toDelete);
		dbExist.put(viewId, dbs);
    	
    	return operation;
    }
    
    /**
     * <p>Keep an eye on the side-effects.</p>
     * 
     * @param random
     * @param dbNo
     * @param viewId
     * @return a generated copy-meta-operation.
     */
    private List<Object> copyOperation(Random random, int dbNo, int viewId) {
    	List<Object> operation = new LinkedList<Object>();
    	
    	operation.add(copy);
    	// get source db
    	List<Object[]> dbs = dbExist.get(viewId);
    	Object[] dbData = dbs.get(random.nextInt(dbs.size()));
    	String sourceDB = (String) dbData[0];
    	int indices = (Integer) dbData[1];
    	operation.add(sourceDB); 	
    	// generate dbName
    	String dbName = dbPrefixes[random.nextInt(dbPrefixes.length)] + dbNo;
		operation.add(dbName);
		
		// update dbExists-map
		dbs.add(new Object[]{dbName,indices});
		dbExist.put(viewId, dbs);
		
		return operation;
    }

    /**
     * <p>Keep an eye on the side-effects.</p>
     * 
     * @param random
     * @param dbNo
     * @param viewId
     * @return a generated create-meta-operation.
     */
    private List<Object> createOperation(Random random, int dbNo, int viewId) {
    	List<Object> operation = new LinkedList<Object>();
    	
    	operation.add(create);
		// generate dbName
		String dbName = dbPrefixes[random.nextInt(dbPrefixes.length)] + dbNo;
		operation.add(dbName);
		// generate indices
		int indices = random.nextInt(MAX_INDICES-MIN_INDICES)+MIN_INDICES;
		operation.add(indices);
		
		// update dbExists-map
		List<Object[]> viewDBs;
		viewDBs = dbExist.get(viewId);
		if (viewDBs==null) viewDBs = new LinkedList<Object[]>();
		
		viewDBs.add(new Object[]{dbName,indices});
		dbExist.put(viewId, viewDBs);
		
		
		
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
     * @param lsn
     * @return a random-generated {@link DatabaseInsertGroup} for directInsert into the BabuDB.
     * @throws Exception
     */
    public InsertGroup getInsertGroup(LSN lsn) throws Exception{
		InsertGroup result;
    	
    	if (lsn.getViewId()>MAX_VIEWID) throw new Exception(lsn.getViewId()+" is a too big viewId, randomGenerator has to be extended.");
		if (lsn.getSequenceNo()>MAX_SEQUENCENO) throw new Exception(lsn.getSequenceNo()+" is a too big sequence number, randomGenerator has to be extended.");
		// setup random with seed from LSN
		Random random = new Random(prims[lsn.getViewId()-1]*lsn.getSequenceNo());
		
		// get the db affected by the insert
		List<Object[]> dbs = dbExist.get(lsn.getViewId());
		assert (dbs!=null) : "Something went wrong. With LSN: "+lsn.toString();
		Object[] db = dbs.get(random.nextInt(dbs.size()));
		String dbName = (String) db[0];
		int dbIndices = (Integer) db[1];
		
		result = this.new InsertGroup(dbName);
		
		// generate some reconstructable key-value-pairs for insert
		int insertsPerGroup = random.nextInt(MAX_INSERTS_PER_GROUP-MIN_INSERTS_PER_GROUP)+MIN_INSERTS_PER_GROUP;
		
		for (int i=0;i<insertsPerGroup;i++){
			int keyLength = random.nextInt(MAX_KEY_LENGTH-MIN_KEY_LENGTH)+MIN_KEY_LENGTH;
			int valueLength = random.nextInt(MAX_VALUE_LENGTH-MIN_VALUE_LENGTH)+MIN_VALUE_LENGTH;
			int index = random.nextInt(dbIndices);
			
			result.addInsert(index, createRandomBytes(random, keyLength), createRandomBytes(random, valueLength));
		}
		
		// generates some deletes from the previous insertGroup, if the same db was chosen
		if (lastISG!=null && lastISG.dbName.equals(dbName)){
			int deletesPerGroup = random.nextInt(MAX_DELETES_PER_GROUP-MIN_DELETES_PER_GROUP)+MIN_DELETES_PER_GROUP;
			int noInserts = lastISG.getNoInserts();
			assert (noInserts>=deletesPerGroup) : "Too many deletes for not enough inserts.";
			
			List<Integer> indices = new LinkedList<Integer>();
			for (int i=0;i<noInserts;i++)
				indices.add(i);
			
			for (int i=0;i<deletesPerGroup;i++){
				int index = indices.remove(random.nextInt(indices.size()));
				
				result.addDelete(lastISG.getIndex(index), lastISG.getKey(index));				
			}
		}
		
		lastISG = result;		
		return result;
    }
    
    public void reset(){
    	lastISG = null;
    }
    
    /**
     * THIS IS FOR THE SLAVES TO CHECK CONSISTENCY
     * 
     * Precondition: RandomGenerator has to be initialized! 
     * 
     * @param lsn
     * @return restores a random GroupInsert for looking it up as a {@link LookupGroup} with directLookup at the BabuDB.
     * @throws Exception
     */
    public LookupGroup getLookupGroup(LSN lsn) throws Exception {
    	LookupGroup result;
    	
    	if (lsn.getViewId()>MAX_VIEWID) throw new Exception(lsn.getViewId()+" is a too big viewId, randomGenerator has to be extended.");
		if (lsn.getSequenceNo()>MAX_SEQUENCENO) throw new Exception(lsn.getSequenceNo()+" is a too big sequence number, randomGenerator has to be extended.");
		// setup random with seed from LSN
		Random random = new Random(prims[lsn.getViewId()-1]*lsn.getSequenceNo());
		
		// get the db affected by the insert
		List<Object[]> dbs = dbExist.get(lsn.getViewId());
		assert (dbs!=null) : "Something went wrong. With LSN: "+lsn.toString();
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
    
    /*
     * (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
    	String string = "RandomGenerator------------------------\n";
    	
    	if (Logging.isDebug()) { 	
	    	string += "The operations scenario:\n";
	    	string += "ViewID | Operation | Parameters\n";
	    	string += "-----------------------------------\n";
	    	for (int i=1;i<=prims.length;i++){
	    		List<List<Object>> ops = operationsScenario.get(i);
	    		for (List<Object> op : ops){
	    			string += i+((i<10) ? "      | " : "     | ");
	    			String opName = ((Operation) op.get(0)).toString();
	    			string += opName+((opName.length()<6) ? "      " : "    ");
	    			for (int y = 1;y<op.size();y++) {
	    				string += " | "+op.get(y);
	    			}
	    			string += "\n";
	    		}
	    		string += "-----------------------------------\n";
	    	}
	    	string += "\n\r";
	    	
	    	if (Logging.isNotice()){
	    		string += "DB history:\n";
	    		string += "ViewID | #Indices | DB Name\n";
	    		for (int i=1;i<=prims.length;i++){
	    			List<Object[]> dbs = dbExist.get(i);
	    			if (dbs!=null) {
		    			for (Object[] db : dbs) {
		    				string += i+((i<10) ? "      | " : "     | ");
		    				int indices = (Integer) db[1];
		    				string += indices+((indices<10) ? "        | " : "       | ");
		    				string += (String) db[0]+"\n";
		    			}
	    			}
	    		}
	    	}
    	}
    	
    	if (lastISG!=null){
    		string += "Last DatabaseInsertGroup:\n";
    		string += lastISG.toString();
    	}
    	
    	return string;
    }
}
