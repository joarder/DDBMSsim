/**
 * @author Joarder Kamal
 */

package jkamal.ddbmssim.workload;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.math3.distribution.ZipfDistribution;
import jkamal.ddbmssim.db.Data;
import jkamal.ddbmssim.db.Database;
import jkamal.ddbmssim.db.Partition;

public class DataPreprocessor {	
	private Map<Integer, Double> zipf_probability_map;
	private Map<Integer, Double> zipf_cumulative_probability_map;
	private Map<Integer, Double> zipf_norm_cumulative_probability_map;
	private Map<Integer, Double> data_weight;
	
	public DataPreprocessor() {		
		this.setZipf_probability_map(new TreeMap<Integer, Double>());
		this.setZipf_cumulative_probability_map(new TreeMap<Integer, Double>());
		this.setZipf_norm_cumulative_probability_map(new TreeMap<Integer, Double>());
		this.setData_weight(new TreeMap<Integer, Double>());
	}
		
	public Map<Integer, Double> getZipf_probability_map() {
		return zipf_probability_map;
	}

	public void setZipf_probability_map(Map<Integer, Double> zipf_probability_map) {
		this.zipf_probability_map = zipf_probability_map;
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
	
	// Calculate zipf probability P(X = x) for all the Data objects in a particular Partition following Zipf Distribution	
	public void getZipfProbability(int seed, int number_of_elements, int data_id_tracker) {
		double exponent = 1.0;
		ZipfDistribution zipf_distribution = new ZipfDistribution(number_of_elements, exponent);
		zipf_distribution.reseedRandomGenerator(seed);
		
		for(int i = data_id_tracker; i <= (number_of_elements + data_id_tracker - 1); i++) {		
			//System.out.println("@ >> "+i+" | "+(i - data_id_tracker + 1));
			double value = zipf_distribution.probability(i - data_id_tracker + 1);
			this.getZipf_probability_map().put(i, value);
		}
	}
	
	// Calculate cumulative zipf probability P(X <= x) for all the Data objects in a particular Partition following Zipf Distribution
	public void getCumulativeProbability(int seed, int number_of_elements, int data_id_tracker) {
		double exponent = 1.0;		
		ZipfDistribution zipf_distribution = new ZipfDistribution(number_of_elements, exponent);
		zipf_distribution.reseedRandomGenerator(seed);
		
		for(int i = data_id_tracker; i <= (number_of_elements + data_id_tracker - 1); i++)
			this.getZipf_cumulative_probability_map().put(i, zipf_distribution.cumulativeProbability(i - data_id_tracker + 1));				
	}
	
	// Calculate cumulative normalised probability P(X <= x) for all the Data objects in a particular Partition following Zipf Distribution
	public double getNormalisedCumulativeProbability(int normalisation_divisor, int data_id_tracker, double carry) {
		double normalised_value = 0.0;
		
		for(int i = data_id_tracker; i <= this.getZipf_cumulative_probability_map().size(); i++) {
			normalised_value = carry + (this.getZipf_cumulative_probability_map().get(i)/normalisation_divisor);
			this.getZipf_norm_cumulative_probability_map().put(i, normalised_value);
		}
		
		return normalised_value;
	}
	
	public Map<Integer, Double> getData_weight() {
		return data_weight;
	}

	public void setData_weight(Map<Integer, Double> data_weight) {
		this.data_weight = data_weight;
	}

	public void preprocess(Database db) {
		Iterator<Partition> iterator = db.getDb_partitions().iterator();
	    Partition partition = null;
	    int partition_size = 0;	  
	    int data_id_tracker = 1;
	    double normalisation_carry_fwd = 0.0;	    
	    
	    // Iterating each partitions
	    while(iterator.hasNext()) {
	    	partition = iterator.next();
	    	partition_size = partition.getPartition_dataSet().size();
	    		    	
	    	this.getZipfProbability(0, partition_size, data_id_tracker);
	    	this.getCumulativeProbability(0, partition_size, data_id_tracker);
	    	normalisation_carry_fwd = this.getNormalisedCumulativeProbability(db.getDb_partitions().size(), data_id_tracker, normalisation_carry_fwd);
	    		 
	    	// Iterating each data objects
	    	for(Data data : partition.getPartition_dataSet()) {	    		
	    		data.setData_zipfProbability(
	    				this.getZipf_probability_map().get(data.getData_id()));
	    		data.setData_cumulativeZipfProbability(
	    				this.getZipf_cumulative_probability_map().get(data.getData_id()));	    		
	    		data.setData_normalisedCumulativeZipfProbability(
	    				this.getZipf_norm_cumulative_probability_map().get(data.getData_id()));	    		
	    		
	    		db.getDb_normalisedCumalitiveZipfProbabilityArray()[data.getData_id()-1] = data.getData_normalisedCumulativeZipfProbability();
	    		data_id_tracker = (data.getData_id() + 1);
	    		
	    		System.out.println(data.getData_id()+" | "
				+data.getData_zipfProbability()+" | "
				+data.getData_cumulativeZipfProbability()+" | "
				+data.getData_normalisedCumulativeZipfProbability());
	    		//System.out.println(">> id = "+(data.getData_id()-1)+" | "+db.getDb_normalised_cumalitive_zipf_probability()[data.getData_id()-1]);
	    	}
	    	
	    	//System.out.println(">> NC = "+normalised_carry_fwd);
	    }	    
	}
}