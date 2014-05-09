/**
 * @author Joarder Kamal
 */

package jkamal.ddbmssim.hgraph;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import jkamal.ddbmssim.db.Database;
import jkamal.ddbmssim.io.StreamCollector;
import jkamal.ddbmssim.main.DBMSSimulator;
import jkamal.ddbmssim.workload.Workload;

public class HGraphMinCut {
	
	/**
	 * shmetis HGraphFile Nparts UBfactor 
	 *  or,
	 * hmetis HGraphFile Nparts UBfactor Nruns CType RType Vcycle Reconst dbglvl
	 *  or,
	 * khmetis HGraphFile Nparts UBfactor Nruns CType OType Vcycle dbglvl 
	 */	
	
	private File exec_dir;
	private String exec_name = null;
	private String num_partitions = null;		
	private List<String> arg_list = new ArrayList<String>();
	
	private String hgraph_file = null;
	private String hgr_fixfile = null;
	private String chgraph_file = null;
	private String chg_fixfile = null;
	
	//public HGraphMinCut(File dir, String hgraph_exec, String hgraph_file, String fix_file, int num_partitions) {
	public HGraphMinCut(Database db, Workload workload, String hgraph_exec, String type, int virtual_data, int partitions) {
		this.exec_dir = new File(DBMSSimulator.hMETIS_DIR_LOCATION);
		this.exec_name = hgraph_exec;
		this.num_partitions = Integer.toString(partitions); //Integer.toString(db.getDb_partitions());
		
		switch(type) {
		case "hgr":
			//this.num_partitions = Integer.toString(partition_numbers);
			this.setHgraph_file(workload.getWrl_id()+"-"+db.getDb_name()+"-"+workload.getWrl_hGraphWorkloadFile());			
			this.setHgr_fixfile(workload.getWrl_id()+"-"+db.getDb_name()+"-"+workload.getWrl_hGraphFixFile());
			System.out.println("[MSG] Workload file: "+this.getHgraph_file());
			System.out.println("[ACT] Running Hyper-graph Partitioning process ...");
			break;
			
		case "chg":
			/*if(virtual_data >= db.getDb_tables().size())
				this.num_partitions = Integer.toString(db.getDb_tables().size());
			else
				this.num_partitions = Integer.toString(virtual_data);
			*/
			//System.out.println("--** virtual data = "+virtual_data+"|"+this.num_partitions);
			this.setChgraph_file(workload.getWrl_id()+"-"+db.getDb_name()+"-"+workload.getWrl_chGraphWorkloadFile());
			this.setChg_fixfile(workload.getWrl_id()+"-"+db.getDb_name()+"-"+workload.getWrl_chGraphFixFile());
			System.out.println("[MSG] Workload file: "+this.getChgraph_file());
			System.out.println("[ACT] Running Hyper-graph Partitioning process ...");
			break;
		}		
		
		switch(this.exec_name) {
		case "shmetis":		
			this.arg_list.add("cmd");
			this.arg_list.add("/c");
			this.arg_list.add("start");
			this.arg_list.add(this.exec_name);		// Executable name
			
			switch(type) {
			case "hgr":
				this.arg_list.add(this.getHgraph_file());			// HGraphFile			
				break;
			case "chg":
				this.arg_list.add(this.getChgraph_file());			// HGraphFile
				break;
			}
			
			this.arg_list.add(this.num_partitions);	// Nparts
			this.arg_list.add("1"); 				// UBfactor(1-49)
			break;
			
		case "hmetis":
			this.arg_list.add("cmd");
			this.arg_list.add("/c");
			this.arg_list.add("start");
			this.arg_list.add(this.exec_name);		// Executable name
			
			switch(type) {
			case "hgr":
				this.arg_list.add(this.getHgraph_file());			// HGraphFile
				//this.arg_list.add(this.getHgr_fixfile());				// FixFile
				break;
			case "chg":
				this.arg_list.add(this.getChgraph_file());			// HGraphFile
				//this.arg_list.add(this.getChg_fixfile());				// FixFile
				break;
			}
			
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
			
			switch(type) {
			case "hgr":
				this.arg_list.add(this.getHgraph_file());			// HGraphFile			
				break;
			case "chg":
				this.arg_list.add(this.getChgraph_file());			// HGraphFile
				break;
			}
			
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
		
	public String getHgr_fixfile() {
		return hgr_fixfile;
	}

	public void setHgr_fixfile(String fixfile) {
		this.hgr_fixfile = fixfile;
	}

	public String getChgraph_file() {
		return chgraph_file;
	}

	public void setChgraph_file(String chgraph_file) {
		this.chgraph_file = chgraph_file;
	}

	public String getChg_fixfile() {
		return chg_fixfile;
	}

	public void setChg_fixfile(String chg_fixfile) {
		this.chg_fixfile = chg_fixfile;
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