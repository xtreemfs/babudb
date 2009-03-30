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
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.xtreemfs.babudb.BabuDBImpl;
import org.xtreemfs.babudb.BabuDBFactory;
import org.xtreemfs.babudb.log.DiskLogger.SyncMode;
import org.xtreemfs.babudb.replication.Replication;
import org.xtreemfs.babudb.sandbox.CLIParser.CliOption;
import org.xtreemfs.include.common.logging.Logging;

/**
 * <p>Tool for setting up a master-BabuDB.</p>
 * 
 * @author flangner
 *
 */
public class SetupMaster {

    /**
     * @param args
     * @throws Exception 
     */
    public static void main(String[] args) throws Exception {
        Logging.start(Logging.LEVEL_ERROR);

        Map<String,CLIParser.CliOption> options = new HashMap<String, CliOption>();
        options.put("path",new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.FILE,new File("/tmp/babudb_benchmark/master")));
        options.put("sync",new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.STRING,"ASYNC"));
        options.put("wait", new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.NUMBER,0));
        options.put("maxq", new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.NUMBER,0));
        options.put("workers", new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.NUMBER,1));
        options.put("thr", new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.NUMBER,1));
        options.put("payload", new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.NUMBER,50));
        options.put("keymin", new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.NUMBER,2));
        options.put("keymax", new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.NUMBER,20));
        options.put("port", new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.NUMBER,0));
        options.put("reset", new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.SWITCH, false));
        options.put("repMode", new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.NUMBER,0));
        options.put("seed", new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.NUMBER,0));
        options.put("h", new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.SWITCH, false));        
        
        List<String> arguments = new ArrayList<String>(1);
        CLIParser.parseCLI(args, options, arguments);

        if ((arguments.size() < 1) || (options.get("h").switchValue))
            usage();
        
        List<InetSocketAddress> slaves = new LinkedList<InetSocketAddress>();    
        if (arguments.get(0).indexOf(",")==-1)
            slaves.add(parseAddress(arguments.get(0)));
        else
            for (String adr : arguments.get(0).split(","))
                slaves.add(parseAddress(adr));
        
        // delete existing files
        if (options.get("reset").switchValue) {
            Process p = Runtime.getRuntime().exec("rm -rf "+options.get("path").fileValue.getAbsolutePath());
            p.waitFor();
        }
        
        BabuDBImpl master = (BabuDBImpl) BabuDBFactory.getMasterBabuDB(
                options.get("path").fileValue.getAbsolutePath(), 
                options.get("path").fileValue.getAbsolutePath(), 
                options.get("workers").numValue.intValue(), 1, 0, 
                SyncMode.valueOf(options.get("sync").stringValue), 
                options.get("wait").numValue.intValue(), 
                options.get("maxq").numValue.intValue(), 
                slaves, 
                options.get("port").numValue.intValue(), null, 
                options.get("repMode").numValue.intValue(), Replication.DEFAULT_MAX_Q*5);
        
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        
        String nextCommand = null;
        while(true) {
            if ((nextCommand = reader.readLine()) != null){
                if (nextCommand.equals("exit")){
                    break;
                } else if(nextCommand.startsWith("benchmark ")){
                    String[] param = nextCommand.split(" ");
                    if (param.length!=3) System.out.println("benchmark #keys prefix \n NOT:"+nextCommand);
                    else { 
                        MasterBabuDBBenchmark b = new MasterBabuDBBenchmark(master,
                                options.get("path").fileValue.getAbsolutePath(),
                                SyncMode.valueOf(options.get("sync").stringValue),
                                options.get("wait").numValue.intValue(),
                                options.get("maxq").numValue.intValue(),
                                options.get("thr").numValue.intValue(),
                                options.get("workers").numValue.intValue(),
                                Integer.parseInt(param[1]),
                                options.get("payload").numValue.intValue(),
                                options.get("keymin").numValue.intValue(),
                                options.get("keymax").numValue.intValue(),
                                options.get("port").numValue.intValue(),param[2],
                                options.get("seed").numValue.longValue());
                        
                        double tpIns = b.benchmarkInserts();
                        double tpIter = b.benchmarkIterate();
                        double durCP = b.checkpoint();
                        double tpLookup = b.benchmarkLookup();
        
                        System.out.println("RESULTS -----------------------------------------\n");
                        
                        System.out.format("total throughput for INSERT : %12.4f keys/s\n", tpIns);
                        System.out.format("total throughput for ITERATE: %12.4f keys/s\n", tpIter);
                        System.out.format("total throughput for LOOKUP : %12.4f keys/s\n\n", tpLookup);
        
                        System.out.format("CHECKPOINTING took          : %12.4f s\n", durCP);
                    }
                } else if(nextCommand.startsWith("sleep")){
                    String[] param = nextCommand.split(" ");
                    if (param.length!=2) System.out.println("wait #seconds\nNOT:"+nextCommand);
                    else Thread.sleep(Long.valueOf(param[1]));
                } else if(nextCommand.startsWith("mode")){
                    String[] param = nextCommand.split(" ");
                    if (param.length!=2) System.out.println("mode #syncSlaves\nNOT:"+nextCommand);
                    else master.replication_switchSyncMode(Integer.parseInt(param[1]));
                } else System.err.println(nextCommand);
            }
        }
        
        Logging.logMessage(Logging.LEVEL_TRACE, master, "master is shutting down...");
        master.shutdown();
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
    
    private static void error(String message) {
        System.err.println(message);
        usage();
    }
    
    public static void usage() {
        System.out.println("SetupMaster <options> <slave_address:port>[,<slave_address:port>]");
        System.out.println("  "+"each thread");
        System.out.println("  "+"<slave_address:port> same as for the master for all available ");
        System.out.println("  "+"slaves seperated by ','");
        System.out.println("  "+"-port where the slave should listen at (default is "+Replication.MASTER_PORT+")");
        System.out.println("  "+"-path directory in which to store the database, default is /tmp/babudb_benchmark/master");
        System.out.println("  "+"-sync synchronization mode FSYNC");
        System.out.println("  "+"-wait ms between to bach writes, default is 0 for synchronous mode");
        System.out.println("  "+"-maxq maxmimum worker queue length, default is 0 for unlimited");
        System.out.println("  "+"-workers number of database worker threads, default is 1");
        System.out.println("  "+"-thr number of threads to use for benchmarking, default is 1");
        System.out.println("  "+"-payload size of values, default is 50 bytes");
        System.out.println("  "+"-keymin minimum key length, default is 2");
        System.out.println("  "+"-keymax maximum key length, default is 20");
        System.out.println("  "+"-repMode 0 means ASYNC; #slaves means SYNC and every value between is NSYNC mode");
        System.out.println("  "+"-seed long value of the seed from which the keys inserted into the db will be generated");
        System.out.println("  "+"-reset starts the DB in clean mode");
        System.exit(1);
    }

}
