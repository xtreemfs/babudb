/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
*/

package org.xtreemfs.babudb.sandbox;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.xtreemfs.babudb.BabuDB;
import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDBFactory;
import org.xtreemfs.babudb.log.DiskLogger.SyncMode;
import org.xtreemfs.babudb.replication.Replication;
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
        Logging.start(Logging.LEVEL_TRACE);

        Map<String,CLIParser.CliOption> options = new HashMap();
        options.put("path",new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.FILE,new File("/tmp/babudb_benchmark/slave")));
        options.put("sync",new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.STRING,"FSYNC"));
        options.put("wait", new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.NUMBER,0));
        options.put("maxq", new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.NUMBER,0));
        options.put("workers", new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.NUMBER,1));
        options.put("port", new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.NUMBER,0));
        options.put("h", new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.SWITCH, false));
        
        List<String> arguments = new ArrayList(2);
        CLIParser.parseCLI(args, options, arguments);

        if ((arguments.size() != 2) || (options.get("h").switchValue))
            usage();
        
        InetSocketAddress master = parseAddress (arguments.get(0));
        List<InetSocketAddress> slaves = new LinkedList<InetSocketAddress>();
        
        if (arguments.get(0).indexOf(",")==-1)
            slaves.add(parseAddress(arguments.get(0)));
        else
            for (String adr : arguments.get(0).split(","))
                slaves.add(parseAddress(adr));
        
        // delete existing files
    //    Process p = Runtime.getRuntime().exec("rm -rf "+options.get("path").fileValue.getAbsolutePath());
    //    p.waitFor();
        
        BabuDB slave = BabuDBFactory.getSlaveBabuDB(
                options.get("path").fileValue.getAbsolutePath(), 
                options.get("path").fileValue.getAbsolutePath(), 
                options.get("workers").numValue.intValue(), 1, 0, 
                SyncMode.valueOf(options.get("sync").stringValue), 
                options.get("wait").numValue.intValue(), 
                options.get("maxq").numValue.intValue(), 
                master, slaves, 
                options.get("port").numValue.intValue(), null);
        
        
        while(true);
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
        System.out.println("SetupSlave <options> <master_address:port> <slave_address:port>[,<slave_address:port>]");
        System.out.println("  "+"<master_address:port> the IP or URL to the master BabuDB and the port it is listening at");
        System.out.println("  "+"<slave_address:port> same as for the master for all available slaves seperated by ','");
        System.out.println("  "+"-path directory in which to store the database, default is /tmp/babudb_benchmark/slave");
        System.out.println("  "+"-sync synchronization mode FSYNC");
        System.out.println("  "+"-wait ms between to batch writes, default is 0 for synchronous mode");
        System.out.println("  "+"-maxq maximum worker queue length, default is 0 for unlimited");
        System.out.println("  "+"-port where the slave should listen at (default is "+Replication.SLAVE_PORT+")");
        System.out.println("  "+"-workers number of database worker threads, default is 1");
        System.exit(1);
    }

}
