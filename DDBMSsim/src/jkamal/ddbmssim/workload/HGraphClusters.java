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
		String wrl_fileName = workload.getWrl_workloadFile();		
		String part_file = wrl_fileName+".part."+partition_numbers;	
		File part = new File(DBMSSimulator.DIR_LOCATION+"\\"+workload.getWrl_id()+"-"+part_file);
		int cluster_id = -1;
		int key = 0;
		
		Scanner scanner = new Scanner(part);
		try {
			while(scanner.hasNextLine()) {
				cluster_id = Integer.valueOf(scanner.nextLine());				
				++key;				
				keyMap.put(key, cluster_id);
				//System.out.println("@debug >> key: "+key+" | Cluster: "+cluster_id);
			}						
		} finally {
			scanner.close();
		}					
		
		//int d = 1;
		//int r = 1;
		Set<Integer> dataSet = new TreeSet<Integer>();
		for(Entry<Integer, ArrayList<Transaction>> entry : workload.getWrl_transactionMap().entrySet()) {
			for(Transaction transaction : entry.getValue()) {		
				for(Integer data_id : transaction.getTr_dataSet()) {
					Data data = db.search(data_id);
					
					//if(data.isData_hasShadowHMetisId()) {
					if(!dataSet.contains(data_id)) {
						//System.out.println("@debug >> d = "+d+"| key: "+keyMap.get(data.getData_shadowHMetisId())+" | "+data.toString());
						data.setData_hmetisClusterId(keyMap.get(data.getData_shadowHMetisId())+1);						
						//System.out.println("@debug >> Data ("+data.toString()+") | hkey: "+data.getData_shadowHMetisId()+" | Cluster: "+data.getData_hmetisClusterId());
						data.setData_shadowHMetisId(-1);
						data.setData_hasShadowHMetisId(false);
						
						dataSet.add(data_id);
						//d++;
					}// else { // Repeated Data
						//System.out.println("@debug >> *Repeated Data ("+data.toString()+") | hkey: "+data.getData_shadowHMetisId());
						//r++;
						//System.out.println("@debug >> *r = "+r);
					//}
				} // end -- for()-Data
			} // end -- for()-Transaction
		} // end -- for()-Transaction Types
	}
}