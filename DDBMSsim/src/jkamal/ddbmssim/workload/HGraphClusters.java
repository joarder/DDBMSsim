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
import jkamal.ddbmssim.main.DBMSSimulator;

public class HGraphClusters {	
	public HGraphClusters() { }
	
	public void readPartFile(Database db, Workload workload, int partition_numbers) throws IOException {		
		Map<Integer, Integer> keyMap = new TreeMap<Integer, Integer>();		
		String wrl_fileName = workload.getWrl_hGraphWorkloadFile();		
		String part_file = wrl_fileName+".part."+partition_numbers;	
		File part = new File(DBMSSimulator.hMETIS_DIR_LOCATION+"\\"+workload.getWrl_id()+"-"+part_file);		
		int key = 1;
		
		Scanner scanner = new Scanner(part);
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
					Data data = db.search(data_id);
					
					if(!dataSet.contains(data_id)) {
						int shadow_id = workload.getWrl_hg_dataId_shadowId_map().get(data.getData_id());
						int cluster_id = keyMap.get(shadow_id)+1;
						//System.out.println("@debug >> "+data.toString()+" | S="+shadow_id+" | C="+cluster_id);
						
						data.setData_hmetisClusterId(cluster_id);
						workload.getWrl_hg_dataId_clusterId_map().put(data.getData_id(), cluster_id);

						data.setData_shadowHMetisId(-1);
						data.setData_hasShadowHMetisId(false);
						
						dataSet.add(data_id);					
					}
				} // end -- for()-Data
			} // end -- for()-Transaction
		} // end -- for()-Transaction Types
	}
}
