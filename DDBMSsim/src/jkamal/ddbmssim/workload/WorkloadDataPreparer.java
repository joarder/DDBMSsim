/**
 * @author Joarder Kamal
 */

package jkamal.ddbmssim.workload;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.commons.math3.distribution.ZipfDistribution;
import jkamal.ddbmssim.db.Data;
import jkamal.ddbmssim.db.Database;
import jkamal.ddbmssim.db.Partition;
import jkamal.ddbmssim.main.DBMSSimulator;

public class WorkloadDataPreparer {	
	private Map<Integer, Integer> zipf_rank_map;
	private Map<Integer, Double> zipf_cumulative_probability_map;
	private Map<Integer, Double> zipf_norm_cumulative_probability_map;
	
	public WorkloadDataPreparer() {		
		this.setZipf_rank_map(new HashMap<Integer, Integer>());
		this.setZipf_cumulative_probability_map(new HashMap<Integer, Double>());
		this.setZipf_norm_cumulative_probability_map(new HashMap<Integer, Double>());
	}
	
	public Map<Integer, Integer> getZipf_rank_map() {
		return this.zipf_rank_map;
	}

	public void setZipf_rank_map(Map<Integer, Integer> zipf_rank_map) {
		this.zipf_rank_map = zipf_rank_map;
	}

	public Map<Integer, Double> getZipf_cumulative_probability_map() {
		return this.zipf_cumulative_probability_map;
	}

	public void setZipf_cumulative_probability_map(
			Map<Integer, Double> zipf_cumulative_probability_map) {
		this.zipf_cumulative_probability_map = zipf_cumulative_probability_map;
	}

	public Map<Integer, Double> getZipf_norm_cumulative_probability_map() {
		return this.zipf_norm_cumulative_probability_map;
	}

	public void setZipf_norm_cumulative_probability_map(
			Map<Integer, Double> zipf_norm_cumulative_probability_map) {
		this.zipf_norm_cumulative_probability_map = zipf_norm_cumulative_probability_map;
	}
	
	// Calculate ranking for all the Data objects in a particular Partition following Zipf Distribution
	public void getZipfRanking(int seed, int number_of_elements, int d_start) {	
		double exponent = 1.0;
		double zipf_random_value = 0.0;
		ArrayList<Integer> zipf_rank_list = new ArrayList<Integer>();		
				
		DBMSSimulator.random_data.reSeed(seed);
		
		for(int i = 0; i < number_of_elements; i++) {
			zipf_random_value = DBMSSimulator.random_data.nextZipf(number_of_elements, exponent);
			zipf_random_value += (d_start-1);
						
			zipf_rank_list.add(((int)zipf_random_value));
		}								
		
		int occurrences = 0;
		for(int i = d_start; i <= (number_of_elements + d_start - 1); i++) {
			occurrences = Collections.frequency(zipf_rank_list, (i));	
			
			this.getZipf_rank_map().put((i), occurrences);						
		}		
	}
	
	// Calculate cumulative probability P(X <= x) for all the Data objects in a particular Partition following Zipf Distribution
	public void getCumulativeProbability(int seed, int number_of_elements, int d_start) {
		double exponent = 1.0;		
		ZipfDistribution zipf_distribution = new ZipfDistribution(number_of_elements, exponent);
		zipf_distribution.reseedRandomGenerator(seed);		
		
		for(int i = d_start; i <= (number_of_elements + d_start - 1); i++)
			this.getZipf_cumulative_probability_map().put((i), zipf_distribution.cumulativeProbability(i - d_start + 1));				
	}
	
	// Calculate cumulative normalised probability P(X <= x) for all the Data objects in a particular Partition following Zipf Distribution
	public void getNormalisedCumulativeProbability(int normalisation_divisor, int d_start) {		
		for(int i = d_start; i <= this.getZipf_cumulative_probability_map().size(); i++)
			this.getZipf_norm_cumulative_probability_map().
				put(i, (this.getZipf_cumulative_probability_map().get(i)/normalisation_divisor));
	}
	
	public void prepareWorkloadData(Database db) {
		Iterator<Partition> iterator = db.getDb_partitions().iterator();
	    Partition partition = null;
	    int partition_size = 0;	  
	    int d_start = 1; //0
	    
	    // Iterating each partitions
	    while(iterator.hasNext()) {
	    	partition = iterator.next();
	    	partition_size = partition.getPartition_dataSet().size();
	    		    	
	    	this.getZipfRanking(
	    			partition.getPartition_id(), partition_size, d_start);
	    	this.getCumulativeProbability(
	    			partition.getPartition_id(), partition_size, d_start);
	    	this.getNormalisedCumulativeProbability(
	    			(db.getDb_partitions().size() - partition.getPartition_id() + 1), d_start);
	    		    	
	    	// Iterating each data objects
	    	for(Data data : partition.getPartition_dataSet()) {	    		
	    		data.setData_ranking(this.getZipf_rank_map().get(data.getData_id())+1); // default Data ranking is 1
	    		data.setData_cumulativeProbability(
	    				Math.round( this.getZipf_cumulative_probability_map().get(data.getData_id()) * 100.0)/100.0);	    		
	    		data.setData_normalisedCumulativeProbability(
	    				Math.round( this.getZipf_norm_cumulative_probability_map().get(data.getData_id()) * 100.0)/100.0);	    		
	    		
	    		System.out.println(data.getData_id()+" "
	    				+data.getData_ranking()+" "
	    				+data.getData_cumulativeProbability()+" "
	    				+data.getData_normalisedCumulativeProbability());
	    		
	    		d_start = (data.getData_id() + 1);
	    	}	    
	    }	    
	}
}