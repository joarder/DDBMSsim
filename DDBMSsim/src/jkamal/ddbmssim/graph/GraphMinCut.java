/**
 * @author Joarder Kamal
 */

package jkamal.ddbmssim.graph;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import jkamal.ddbmssim.db.Database;
import jkamal.ddbmssim.io.StreamCollector;
import jkamal.ddbmssim.main.DBMSSimulator;
import jkamal.ddbmssim.workload.Workload;

public class GraphMinCut {
	
	/**
	 * pmetis GraphFile Nparts
	 *  or,
	 * khmetis GraphFile Nparts 
	 */	
	
	private File exec_dir;
	private String exec_name = null;
	private String num_partitions = null;		
	private String graph_file = null;
	private List<String> arg_list = new ArrayList<String>();		

	public GraphMinCut(Database db, Workload workload, String graph_exec, int partitions) {
		this.exec_dir = new File(DBMSSimulator.METIS_DIR_LOCATION);
		this.exec_name = graph_exec;
		this.num_partitions = Integer.toString(partitions); //Integer.toString(db.getDb_partitions());
		this.setGraph_file(workload.getWrl_id()+"-"+db.getDb_name()+"-"+workload.getWrl_graphWorkloadFile()); 
		
		System.out.println("[MSG] Workload file: "+this.getGraph_file());
		System.out.println("[ACT] Starting Graph Partitioning process ...");
		
		switch(this.exec_name) {
		case "pmetis":		
			this.arg_list.add("cmd");
			this.arg_list.add("/c");
			//this.arg_list.add("start");
			this.arg_list.add(this.exec_name);			// Executable name
			this.arg_list.add(this.getGraph_file());	// GraphFile
			this.arg_list.add(this.num_partitions);		// Nparts
			break;
		
		case "kmetis":
			this.arg_list.add("cmd");
			this.arg_list.add("/c");
			//this.arg_list.add("start");
			this.arg_list.add(this.exec_name);			// Executable name
			this.arg_list.add(this.getGraph_file());	// GraphFile
			this.arg_list.add(this.num_partitions);		// Nparts				
			break;
		}								
	}		
	
	public String getGraph_file() {
		return graph_file;
	}

	public void setGraph_file(String graph_file) {
		this.graph_file = graph_file;
	}

	public void runMetis() throws IOException {		
		String[] args = this.arg_list.toArray(new String[this.arg_list.size()]);				
		
		ProcessBuilder pb = new ProcessBuilder(args);		
		pb.directory(exec_dir);
//		Process p = pb.start();
//		
//		// Any error? Or, Any output? 
//		StreamCollector errorStreams = new StreamCollector(p.getErrorStream(), "ERROR");
//		StreamCollector outputStreams = new StreamCollector(p.getInputStream(), "OUTPUT");
//		// Start stream collectors
//		outputStreams.start();
//		errorStreams.start();
		
		try {
			final Process p = pb.start();						
			
			new Thread(new Runnable(){
				public void run(){
		        	Scanner stdin = new Scanner(p.getInputStream());
		            while(stdin.hasNextLine()){
		            	System.out.println(stdin.nextLine());
		            }
		            
		            stdin.close();
				}
		    }).start();
			
			// Any error? Or, Any output? 
			//StreamCollector errorStreams = new StreamCollector(p.getErrorStream(), "ERROR");
			//StreamCollector outputStreams = new StreamCollector(p.getInputStream(), "OUTPUT");
			
			// Start stream collectors
			//outputStreams.start();
			//errorStreams.start();
			
			p.waitFor();
			
			//PrintWriter printer = new PrintWriter(outputStreams);
			
		} catch (IOException | InterruptedException e) {						
			e.printStackTrace();
		}
	}	
}