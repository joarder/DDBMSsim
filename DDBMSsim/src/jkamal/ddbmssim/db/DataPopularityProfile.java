/**
 * @author Joarder Kamal
 */

package jkamal.ddbmssim.db;

import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.math3.distribution.ZipfDistribution;

public class DataPopularityProfile {
	private Map<Integer, Double> zipf_cumulative_probability_map;	
	
	public DataPopularityProfile() {		
		this.setZipf_cumulative_probability_map(new TreeMap<Integer, Double>());
	}

	public Map<Integer, Double> getZipf_cumulative_probability_map() {
		return this.zipf_cumulative_probability_map;
	}

	public void setZipf_cumulative_probability_map(
			Map<Integer, Double> zipf_cumulative_probability_map) {
		this.zipf_cumulative_probability_map = zipf_cumulative_probability_map;
	}
	
	//Generates Zipf Ranks for the Primary Tables
	public void generateDataPopularity(Database db) {
		double exponent = 2.0; // 2~3
		for(Table table : db.getDb_tables()) {
	    	if(table.getTbl_type() == 0) {
	    		this.getZipfProbability(table, exponent);
	    		
	    		//for(int i = 1; i <= table.getTbl_data_count(); i++)
	    			//System.out.println("--> rank("+i+") | PK="+table.getTbl_data_rank()[i]);
	    	}
		}
	}
	
	// Calculate zipf probability P(X = x) for all the Data objects in a particular Partition following Zipf Distribution	
	private void getZipfProbability(Table table, double exponent) {		
		ZipfDistribution zipf_distribution = new ZipfDistribution(table.getTbl_data_count(), exponent);
		zipf_distribution.reseedRandomGenerator(0);
				
		for(int d = table.getTbl_data_count(); d >= 1; d--) {
			//double probability = zipf_distribution.probability(d);
			
			double cumulative_probability = zipf_distribution.cumulativeProbability(d);
			this.getZipf_cumulative_probability_map().put(d, cumulative_probability);
			
			//System.out.println("@ "+d+"|P="+probability+"|CP="+cumulative_probability);
		}
		
		//System.out.println("@ --<"+table.getTbl_name()+">--");
		
		int d = 1;
		for(Partition partition : table.getTbl_partitions()) {
			for(Data data : partition.getPartition_dataSet()) {
				data.setData_cumulativeZipfProbability(
						this.getZipf_cumulative_probability_map().get((d % table.getTbl_data_count())+1));
				
				data.setData_rank((d % table.getTbl_data_count())+1);
				table.getTbl_data_rank()[(d % table.getTbl_data_count())+1] = data.getData_primary_key();
				
				/*System.out.println("@ "+data.toString()+" | Rank = "+data.getData_rank()+" | CP = "+data.getData_cumulativeZipfProbability()
						+" | table_data_rank_pos = "+((d % table.getTbl_data_count())+1)
						+" | table_data_rank_val = "+table.getTbl_data_rank()[(d % table.getTbl_data_count())+1]);
				*/
				
				d++;
			}
		}
	}		
}