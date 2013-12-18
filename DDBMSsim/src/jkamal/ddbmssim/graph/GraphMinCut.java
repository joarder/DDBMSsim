/**
 * @author Joarder Kamal
 */

package jkamal.ddbmssim.graph;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import jkamal.ddbmssim.io.StreamCollector;
import jkamal.ddbmssim.main.DBMSSimulator;
import jkamal.ddbmssim.workload.Workload;

public class GraphMinCut {
	
	/**
	 * pmetis HGraphFile Nparts UBfactor 
	 *  or,
	 * khmetis HGraphFile Nparts UBfactor Nruns CType OType Vcycle dbglvl 
	 */	
	
	private File exec_dir;
	private String exec_name = null;
	private String num_partitions = null;		
	private List<String> arg_list = new ArrayList<String>();
	private String hgraph_file = null;
	private String fixfile = null;

	//public HGraphMinCut(File dir, String hgraph_exec, String hgraph_file, String fix_file, int num_partitions) {
	public GraphMinCut(Workload workload, String hgraph_exec, int partition_numbers) {
		this.exec_dir = new File(DBMSSimulator.hMETIS_DIR_LOCATION);
		this.exec_name = hgraph_exec;
		this.num_partitions = Integer.toString(partition_numbers);
		this.setHgraph_file(workload.getWrl_id()+"-"+workload.getWrl_workloadFile());
		System.out.println("@ - "+this.getHgraph_file());
		this.setFixfile(workload.getWrl_fixFile());		
		
		switch(this.exec_name) {
		case "shmetis":		
			this.arg_list.add("cmd");
			this.arg_list.add("/c");
			this.arg_list.add("start");
			this.arg_list.add(this.exec_name);		// Executable name
			this.arg_list.add(this.getHgraph_file());			// HGraphFile
			this.arg_list.add(this.num_partitions);	// Nparts
			this.arg_list.add("1"); 				// UBfactor(1-49)
			break;
			
		case "hmetis":
			this.arg_list.add("cmd");
			this.arg_list.add("/c");
			this.arg_list.add("start");
			this.arg_list.add(this.exec_name);		// Executable name
			this.arg_list.add(this.getHgraph_file());			// HGraphFile
			//this.arg_list.add(this.getFixfile());				// FixFile
			this.arg_list.add(this.num_partitions);	// Nparts
			this.arg_list.add("1");					// UBfactor(1-49)
			this.arg_list.add("10");				// Nruns(>=1)
			this.arg_list.add("1");					// CType(1-5)
			this.arg_list.add("1");					// RType(1-3)
			this.arg_list.add("1");					// Vcycle(0-3)
			this.arg_list.add("0");					// Reconst(0-1)
			this.arg_list.add("0");					// dbglvl(0+1+2+4+8+16)
			break;
		
		case "khmetis":
			this.arg_list.add("cmd");
			this.arg_list.add("/c");
			this.arg_list.add("start");
			this.arg_list.add(this.exec_name);		// Executable name
			this.arg_list.add(this.getHgraph_file());			// HGraphFile
			this.arg_list.add(this.num_partitions);	// Nparts
			this.arg_list.add("5");					// UBfactor(>=5)
			this.arg_list.add("10");				// Nruns(>=1)
			this.arg_list.add("1");					// CType(1-5)
			this.arg_list.add("1");					// OType(1-2) -- 1: Minimizes the hyper edge cut, 2: Minimizes the sum of external degrees (SOED)
			this.arg_list.add("0");					// Vcycle(0-3)
			this.arg_list.add("0");					// dbglvl(0+1+2+4+8+16)				
			break;
		}								
	}		
	
	public String getHgraph_file() {
		return hgraph_file;
	}

	public void setHgraph_file(String hgraph_file) {
		this.hgraph_file = hgraph_file;
	}
		
	public String getFixfile() {
		return fixfile;
	}

	public void setFixfile(String fixfile) {
		this.fixfile = fixfile;
	}

	public void runHMetis() throws IOException {
		String[] args = arg_list.toArray(new String[arg_list.size()]);
		
		ProcessBuilder pb = new ProcessBuilder(args);
		pb.directory(exec_dir);
		Process p = pb.start();
		
		// Any error? Or, Any output? 
		StreamCollector errorStreams = new StreamCollector(p.getErrorStream(), "ERROR");
		StreamCollector outputStreams = new StreamCollector(p.getInputStream(), "OUTPUT");
		// Start stream collectors
		outputStreams.start();
		errorStreams.start();
	}
}