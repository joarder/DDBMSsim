/**
 * @author Joarder Kamal
 */

package jkamal.ddbmssim.db;

import java.util.Set;
import java.util.TreeSet;

public class Table implements Comparable<Table>{
	private int tbl_id;
	private int tbl_db_id;
	private String tbl_name;
	private int tbl_type;
	private int tbl_data_count;
	private double tbl_size;
	private Set<Partition> tbl_partitions;
	private double tbl_max_cp;
	private double tbl_min_cp;
	
	public Table(int id, int type, int db_id, String name) {
		this.setTbl_id(id);
		this.setTbl_db_id(db_id);
		this.setTbl_name(name);
		this.setTbl_type(type);
		this.setTbl_data_count(0);
		this.setTbl_size(0.0d);
		this.setTbl_partitions(new TreeSet<Partition>());
		this.setTbl_max_cp(Double.MIN_VALUE);
		this.setTbl_min_cp(Double.MAX_VALUE);
	}
	
	// Copy Constructor
	public Table(Table table) {
		this.setTbl_id(table.getTbl_id());
		this.setTbl_db_id(table.getTbl_db_id());
		this.setTbl_name(table.getTbl_name());
		this.setTbl_type(table.getTbl_type());
		this.setTbl_data_count(table.getTbl_data_count());
		this.setTbl_size(table.getTbl_size());
		
		Set<Partition> clone_partitions = new TreeSet<Partition>();		
		for(Partition clonePartition : table.getTbl_partitions())
			clone_partitions.add(clonePartition);
		this.setTbl_partitions(clone_partitions);
		
		this.setTbl_max_cp(table.getTbl_max_cp());
		this.setTbl_min_cp(table.getTbl_min_cp());
	}
	
	public int getTbl_id() {
		return tbl_id;
	}

	public void setTbl_id(int tbl_id) {
		this.tbl_id = tbl_id;
	}

	public int getTbl_db_id() {
		return tbl_db_id;
	}

	public void setTbl_db_id(int tbl_db_id) {
		this.tbl_db_id = tbl_db_id;
	}

	public String getTbl_name() {
		return tbl_name;
	}

	public void setTbl_name(String tbl_name) {
		this.tbl_name = tbl_name;
	}

	public int getTbl_data_count() {
		return tbl_data_count;
	}

	public void setTbl_data_count(int tbl_data_count) {
		this.tbl_data_count = tbl_data_count;
	}

	public double getTbl_size() {
		return tbl_size;
	}

	public void setTbl_size(double tbl_size) {
		this.tbl_size = tbl_size;
	}

	public int getTbl_type() {
		return tbl_type;
	}

	public void setTbl_type(int tbl_type) {
		this.tbl_type = tbl_type;
	}

	public Set<Partition> getTbl_partitions() {
		return tbl_partitions;
	}

	public void setTbl_partitions(Set<Partition> tbl_partitions) {
		this.tbl_partitions = tbl_partitions;
	}
	
	public double getTbl_max_cp() {
		return tbl_max_cp;
	}

	public void setTbl_max_cp(double tbl_max_cp) {
		this.tbl_max_cp = tbl_max_cp;
	}

	public double getTbl_min_cp() {
		return tbl_min_cp;
	}

	public void setTbl_min_cp(double tbl_min_cp) {
		this.tbl_min_cp = tbl_min_cp;
	}

	public Partition getPartition(int partition_id) {// search by local partition id from the Table level		
		for(Partition partition : this.getTbl_partitions()) {						
			if(partition.getPartition_id() == partition_id) 
				return partition;
		}	
		
		return null;
	}
	
	public void updateTableLoad() {
		double sum = 0.0d;
		for(Partition partition : this.getTbl_partitions()) {
			sum += partition.getPartition_size();
		}
		
		this.setTbl_size(sum);
	}

	public void show() {
		
	}
	
	@Override
	public String toString() {
		return (this.getTbl_name()+"|"+this.getTbl_size()+" MB");
	}
	
	@Override
	public boolean equals(Object tbl) {
		if (!(tbl instanceof Table)) {
			return false;
		}
		
		Table table = (Table) tbl;
		return (this.getTbl_name().equals(table.getTbl_name()));
	}

	@Override
	public int hashCode() {
		return (this.getTbl_name().hashCode());
	}

	@Override
	public int compareTo(Table table) {		
		return (((int)this.getTbl_id() < (int)table.getTbl_id()) ? -1: 
			((int)this.getTbl_id() > (int)table.getTbl_id()) ? 1:0);		
	}	
}