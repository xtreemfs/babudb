/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */

package org.xtreemfs.babudb.sandbox;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDBFactory;
import org.xtreemfs.babudb.BabuDBImpl;
import org.xtreemfs.babudb.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.log.DiskLogger.SyncMode;
import org.xtreemfs.babudb.replication.Replication;
import org.xtreemfs.babudb.sandbox.BenchmarkWorkerThread.BenchmarkOperation;
import org.xtreemfs.babudb.sandbox.CLIParser.CliOption;
import org.xtreemfs.include.common.logging.Logging;

/**
 * <p>Tool for setting up a slave-BabuDB.</p>
 * 
 * @author flangner
 *
 */
public class SetupSlave {

    /**
     * @param args
     * @throws BabuDBException 
     * @throws IOException 
     * @throws InterruptedException 
     */
    public static void main(String[] args) throws BabuDBException, IOException, InterruptedException {
        Logging.start(Logging.LEVEL_ERROR);

        Map<String,CLIParser.CliOption> options = new HashMap<String, CliOption>();
        options.put("path",new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.FILE,new File("/tmp/babudb_benchmark/slave")));
        options.put("sync",new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.STRING,"FSYNC"));
        options.put("wait", new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.NUMBER,0));
        options.put("maxq", new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.NUMBER,0));
        options.put("workers", new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.NUMBER,1));
        options.put("port", new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.NUMBER,0));
        options.put("h", new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.SWITCH, false));
        options.put("keymin", new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.NUMBER,2));
        options.put("keymax", new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.NUMBER,20));
        options.put("reset", new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.SWITCH, false));
        
        List<String> arguments = new ArrayList<String>(2);
        CLIParser.parseCLI(args, options, arguments);

        if ((arguments.size() < 2) || (options.get("h").switchValue))
            usage();
        
        InetSocketAddress master = parseAddress (arguments.get(0));
        List<InetSocketAddress> slaves = new LinkedList<InetSocketAddress>();
        
        if (arguments.get(1).indexOf(",")==-1)
            slaves.add(parseAddress(arguments.get(1)));
        else
            for (String adr : arguments.get(1).split(","))
                slaves.add(parseAddress(adr));
        
        // delete existing files
        if (options.get("reset").switchValue) {
            Process p = Runtime.getRuntime().exec("rm -rf "+options.get("path").fileValue.getAbsolutePath());
            p.waitFor();
        }
        
        BabuDBImpl slave = (BabuDBImpl) BabuDBFactory.getSlaveBabuDB(
                options.get("path").fileValue.getAbsolutePath(), 
                options.get("path").fileValue.getAbsolutePath(), 
                options.get("workers").numValue.intValue(), 1, 0, 
                SyncMode.valueOf(options.get("sync").stringValue), 
                options.get("wait").numValue.intValue(), 
                options.get("maxq").numValue.intValue(), 
                master, slaves, 
                options.get("port").numValue.intValue(), null, 0);
        
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        
        String nextCommand = null;
        while(true) {
            if ((nextCommand = reader.readLine()) != null){
                if (nextCommand.equals("exit")){
                    break;
                } else if(nextCommand.startsWith("consistencyCheck")){
                    String[] param = nextCommand.split(" ");
                    if (param.length!=6) System.out.println("consistencyCheck dbName indexId value #keys seed\nNOT:"+nextCommand);
                    else {
                        byte[] payload = getPayload(Integer.parseInt(param[3]));
                        int keys = Integer.parseInt(param[4]);
                        BenchmarkWorkerThread w = new BenchmarkWorkerThread(1, keys, options.get("keymin").numValue.intValue(),
                                options.get("keymax").numValue.intValue(), payload, slave, BenchmarkOperation.ITERATE, "", Long.valueOf(param[5]));
                        
                        for (int i = 0;i<keys;i++){
                            byte[] key = w.createRandomKey();
                            byte[] result = slave.hiddenLookup(param[1], Integer.parseInt(param[2]), key);
                            
                            if (result==null) {
                                String msg = "No result for key: "+new String(key);
                                Logging.logMessage(Logging.LEVEL_ERROR, slave, msg);
                                slave.shutdown();
                                throw new BabuDBException(ErrorCode.REPLICATION_FAILURE,msg);
                            } else if (!new String(result).equals(new String(payload))){
                                String msg = "Consistency check for slave failed! expected: "+new String(payload)+" found: "+new String (result)+" request: "+nextCommand;
                                Logging.logMessage(Logging.LEVEL_ERROR, slave, msg);
                                slave.shutdown();
                                throw new BabuDBException(ErrorCode.REPLICATION_FAILURE,msg);
                            }
                        }
                        System.out.println("CONSISTENCY CHECK WAS SUCCESSFUL!");
                    }
                } else if(nextCommand.startsWith("sleep")){
                    String[] param = nextCommand.split(" ");
                    if (param.length!=2) System.out.println("wait #seconds\nNOT:"+nextCommand);
                    else {
                        Thread.sleep(Long.valueOf(param[1]));
                    }
                } else System.err.println("UNKNOWN COMMAND: "+nextCommand);
            }
        }
        
        Logging.logMessage(Logging.LEVEL_TRACE, slave, "slave is shutting down...");
        slave.shutdown();
    }
    
    private static InetSocketAddress parseAddress (String adr){
        String[] comp = adr.split(":");
        if (comp.length!=2){
            error("Address '"+adr+"' is illegal!");
            return null;
        }
        
        try {
            int port = Integer.parseInt(comp[1]);
            return new InetSocketAddress(comp[0],port);
        } catch (NumberFormatException e) {
            error("Address '"+adr+"' is illegal! Because: "+comp[1]+" is not a number.");
            return null;
        }      
    }
    
    private static byte[] getPayload (int valueLength){
        byte[] payload = new byte[valueLength];
        for (int i = 0; i < valueLength; i++)
            payload[i] = 'v';
        
        return payload;
    }
    private static void error(String message) {
        System.err.println(message);
        usage();
    }
    
    public static void usage() {
        System.out.println("SetupSlave <options> <master_address:port> <slave_address:port>[,<slave_address:port>]");
        System.out.println("  "+"<master_address:port> the IP or URL to the master BabuDB and the port it is listening at");
        System.out.println("  "+"<slave_address:port> same as for the master for all available slaves seperated by ','");
        System.out.println("  "+"-path directory in which to store the database, default is /tmp/babudb_benchmark/slave");
        System.out.println("  "+"-sync synchronization mode FSYNC");
        System.out.println("  "+"-keymin minimum key length, default is 2");
        System.out.println("  "+"-keymax maximum key length, default is 20");
        System.out.println("  "+"-wait ms between to batch writes, default is 0 for synchronous mode");
        System.out.println("  "+"-maxq maximum worker queue length, default is 0 for unlimited");
        System.out.println("  "+"-port where the slave should listen at (default is "+Replication.SLAVE_PORT+")");
        System.out.println("  "+"-workers number of database worker threads, default is 1");
        System.out.println("  "+"-reset starts the DB in clean mode");
        System.exit(1);
    }

}
