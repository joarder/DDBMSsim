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
import jkamal.ddbmssim.db.Table;
import jkamal.ddbmssim.main.DBMSSimulator;

public class DataPopularityProfile {
	private Map<Integer, Double> zipf_exponent;
	private Map<Integer, Double> zipf_probability_map;
	private Map<Integer, Double> zipf_cumulative_probability_map;
	private Map<Integer, Double> zipf_norm_cumulative_probability_map;	
	
	public DataPopularityProfile() {
		this.setZipf_exponent(new TreeMap<Integer, Double>());
		this.setZipf_probability_map(new TreeMap<Integer, Double>());
		this.setZipf_cumulative_probability_map(new TreeMap<Integer, Double>());
		this.setZipf_norm_cumulative_probability_map(new TreeMap<Integer, Double>());
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
	
	public Map<Integer, Double> getZipf_exponent() {
		return zipf_exponent;
	}

	public void setZipf_exponent(Map<Integer, Double> zipf_exponent) {
		this.zipf_exponent = zipf_exponent;
	}

	// Calculate zipf probability P(X = x) for all the Data objects in a particular Partition following Zipf Distribution	
	public void getZipfProbability(int seed, int number_of_elements, int data_id_tracker, double exponent) {		
		ZipfDistribution zipf_distribution = new ZipfDistribution(number_of_elements, exponent);
		zipf_distribution.reseedRandomGenerator(seed);
		
		for(int i = data_id_tracker; i <= (number_of_elements + data_id_tracker - 1); i++) {			
			double probability = zipf_distribution.probability(i - data_id_tracker + 1);
			double cumulative_probability = zipf_distribution.cumulativeProbability(i - data_id_tracker + 1);
			
			this.getZipf_probability_map().put(i, probability);
			this.getZipf_cumulative_probability_map().put(i, cumulative_probability);
		}
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

	//
	public void generateDataPopularity(Database db) {			  
	    int data_id_tracker = 1;
	    double normalisation_carry_fwd = 0.0;
	    
	    this.configureZipfExponent(db);
	    
	    for(Table tbl : db.getDb_tables()) {
	    	//int table_data_size = DBMSSimulator.TPCC_TABLE[tbl.getTbl_id()-1];
	    	int table_data_size = tbl.getTbl_data_count();
	    	
	    	this.getZipfProbability(0, table_data_size, data_id_tracker, this.getZipf_exponent().get(tbl.getTbl_id()));	    	
	    	normalisation_carry_fwd = this.getNormalisedCumulativeProbability(db.getDb_tables().size(), data_id_tracker, normalisation_carry_fwd);
	    		    	
	    	data_id_tracker += table_data_size;
		}
		
		// Iterating each tables
	    data_id_tracker = 1;
		Iterator<Table> t_iterator = db.getDb_tables().iterator();
	    while(t_iterator.hasNext()) {
	    	Table table = t_iterator.next();
	    	
	    	// Iterating each partitions
	    	Iterator<Partition> p_iterator = table.getTbl_partitions().iterator();
		    while(p_iterator.hasNext()) {
		    	Partition partition = p_iterator.next();
	   		 
		    	// Iterating each data objects
		    	for(Data data : partition.getPartition_dataSet()) {	
		    		//System.out.println(data.getData_id()+"|"+data_id_tracker);
		    		data.setData_zipfProbability(
		    				this.getZipf_probability_map().get(data_id_tracker));
		    		data.setData_cumulativeZipfProbability(
		    				this.getZipf_cumulative_probability_map().get(data_id_tracker));	    		
		    		data.setData_normalisedCumulativeZipfProbability(
		    				this.getZipf_norm_cumulative_probability_map().get(data_id_tracker));	    		
		    		
		    		db.getDb_normalisedCumalitiveZipfProbabilityArray()[data.getData_id()-1] = data.getData_normalisedCumulativeZipfProbability();
		    		if(data.getData_id() == 206) {
		    		System.out.println(data.getData_id()+" | "
					+data.getData_zipfProbability()+" | "
					+data.getData_cumulativeZipfProbability()+" | "
					+data.getData_normalisedCumulativeZipfProbability());}
		    				    		
		    		++data_id_tracker;
		    		
		    		
		    		if(data.getData_normalisedCumulativeZipfProbability() > table.getTbl_max_cp())
						table.setTbl_max_cp(data.getData_normalisedCumulativeZipfProbability());
					
					if(data.getData_normalisedCumulativeZipfProbability() < table.getTbl_min_cp())
						table.setTbl_min_cp(data.getData_normalisedCumulativeZipfProbability());
		    	}		    	
		    }
	    }			
	}
	
	private void configureZipfExponent(Database db) {		
		for(Table tbl : db.getDb_tables()) {
			int table_data_size = DBMSSimulator.TPCC_TABLE[tbl.getTbl_id()-1];
			
			if(tbl.getTbl_id() == 2 || tbl.getTbl_id() == 4 || tbl.getTbl_id() == 9)
				this.getZipf_exponent().put(tbl.getTbl_id(), 2.25d);	    		
			else
				this.getZipf_exponent().put(tbl.getTbl_id(), 2.25d+scale(table_data_size, 0, 1, 1, 300));						
		}	
	}
	
	private double scale(double x, double a, double b, double min, double max) {
		return ((((b - a) * (x - min))/(max - min)) + a);
	}
}