/**
 * @author Joarder Kamal
 */

package jkamal.ddbmssim.workload;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.TreeSet;
import jkamal.ddbmssim.db.Data;
import jkamal.ddbmssim.db.Database;

public class ClusterIdMapper {	
	public ClusterIdMapper() { }
	
	public void processPartFile(Database db, Workload workload, int partition_numbers, String dir, String partitioner, int virtual_data) throws IOException {		
		Map<Integer, Integer> keyMap = new TreeMap<Integer, Integer>();		
		String wrl_file_name = null;
		String part_file_name = null;
		
		switch(partitioner) {
		case "hgr":
			wrl_file_name = workload.getWrl_id()+"-"+db.getDb_name()+"-"+workload.getWrl_hGraphWorkloadFile();
			//part_file_name = wrl_file_name+".part."+partition_numbers;
			break;
		case "chg":			
			wrl_file_name = workload.getWrl_id()+"-"+db.getDb_name()+"-"+workload.getWrl_chGraphWorkloadFile();
			/*
			if(virtual_data >= db.getDb_tables().size())
				part_file_name = wrl_file_name+".part."+db.getDb_tables().size();
			else
				part_file_name = wrl_file_name+".part."+virtual_data;
			*/
			break;
		case "gr":			
			wrl_file_name = workload.getWrl_id()+"-"+db.getDb_name()+"-"+workload.getWrl_graphWorkloadFile();
			//part_file_name = wrl_file_name+".part."+partition_numbers;
			break;
		}
		
		part_file_name = wrl_file_name+".part."+partition_numbers;	
		/*File file = new File(dir+"\\"+part_file_name);
		
		if(!file.exists() && partitioner == "chg") {
			if(virtual_data >= db.getDb_tables().size())
				part_file_name = wrl_file_name+".part."+(db.getDb_tables().size()-1);
			else
				part_file_name = wrl_file_name+".part."+(virtual_data-1);
		}*/
			
		File part_file = new File(dir+"\\"+part_file_name);
		
		int key = 1;		
		//System.out.println("@ - "+part_file_name);
		Scanner scanner = new Scanner(part_file);
		try {
			while(scanner.hasNextLine()) {
				int cluster_id = Integer.valueOf(scanner.nextLine());								
				keyMap.put(key, cluster_id);	//System.out.println("@debug >> key: "+key+" | Cluster: "+cluster_id);				
				++key;
			}						
		} finally {
			scanner.close();
		}					
		
		Set<Integer> dataSet = new TreeSet<Integer>();
		for(Entry<Integer, ArrayList<Transaction>> entry : workload.getWrl_transactionMap().entrySet()) {
			for(Transaction transaction : entry.getValue()) {		
				for(Integer data_id : transaction.getTr_dataSet()) {
					Data data = db.getData(data_id);
					
					if(!dataSet.contains(data_id)) {
						int shadow_id = workload.getWrl_dataId_shadowId_map().get(data.getData_id());
						int cluster_id = -1;
						int virtual_id = data.getData_virtual_data_id();
						
						switch(partitioner) {
						case "hgr":
							cluster_id = keyMap.get(shadow_id)+1;
							data.setData_hmetisClusterId(cluster_id);
							workload.getWrl_hg_dataId_clusterId_map().put(data.getData_id(), cluster_id);
							break;
							
						case "chg":
							/*int x = 0;
							if(virtual_data >= db.getDb_tables().size())
								x = (virtual_id % db.getDb_tables().size())+1;
							else
								x = (virtual_id % virtual_data)+1;
							*/
							//System.out.println(">> x="+"|keyMap.get(x)="+keyMap.get(x));
							//cluster_id = (keyMap.get(x)*3)+1;
							cluster_id = keyMap.get(virtual_id)+1;
							data.setData_chmetisClusterId(cluster_id);
							workload.getWrl_chg_virtualDataId_clusterId_map().put(data.getData_virtual_data_id(), cluster_id);
							workload.getWrl_chg_dataId_clusterId_map().put(data.getData_id(), cluster_id);
							break;
							
						case "gr":
							cluster_id = keyMap.get(shadow_id)+1;
							data.setData_metisClusterId(cluster_id);
							workload.getWrl_gr_dataId_clusterId_map().put(data.getData_id(), cluster_id);
							break;
						}						

						//System.out.println("@debug >> "+data.toString()+" | S="+shadow_id+" | C="+cluster_id+" | V="+virtual_id);
						
						data.setData_shadowId(-1);
						data.setData_hasShadowId(false);
						
						dataSet.add(data_id);					
					}
				} // end -- for()-Data
			} // end -- for()-Transaction
		} // end -- for()-Transaction Types
	}
}
