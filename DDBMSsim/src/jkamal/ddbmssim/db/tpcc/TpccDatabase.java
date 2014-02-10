/**
 * @author Joarder Kamal
 */

package jkamal.ddbmssim.db.tpcc;

import jkamal.ddbmssim.db.Database;

public class TpccDatabase extends Database {
	private double[] tpcc_table_dist = {0.0019, 0.0192, 0.0192, 0.192, 0.5769, 0.0576, 0.0576, 0.0173, 0.0576};
	private int[] tpcc_table = {30, 10, 30, 100, 5, 30, 300, 100, 1};
	
	// Customer	(W*30K)
	// District (W*10)
	// History (W*30K)
	// Item (100K)
	// New-Order (W*5K)
	// Order (W*30K)
	// Order-Line (W*300K)
	// Stock (W*100K)
	// Warehouse (W)
	
	public TpccDatabase(Database db) {
		super(db);		
	}
}