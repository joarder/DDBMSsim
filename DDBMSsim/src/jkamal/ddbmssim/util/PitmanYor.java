/**
 * @author Joarder Kamal
 */

package jkamal.ddbmssim.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.math3.distribution.BetaDistribution;
import org.apache.commons.math3.random.RandomDataGenerator;
 
public class PitmanYor {
	private double _d; // 0 <= d < 1
	private double _alpha; // Concentration parameter, alpha > -d
	
	public double get_d() {
		return _d;
	}

	public void set_d(double _d) {
		this._d = _d;
	}

	public double get_alpha() {
		return _alpha;
	}

	public void set_alpha(double _alpha) {
		this._alpha = _alpha;
	}

	public PitmanYor(double d, double alpha) {
		this.set_d(d);
		this.set_alpha(alpha);
	}
	
	public static void main(String[] args) {
		// py(d, alpha) -- py(1/a, 0.5) where a = 2
		// a is the power-law parameter (a > 1)
		PitmanYor py = new PitmanYor(0.5, 0.5);
		int n = 1000; // total number of data objects
		double sum = 0.0;
				
		Map<Integer, Double> beta_map = new TreeMap<Integer, Double>();
		Map<Integer, Double> pi_map = new TreeMap<Integer, Double>();
		
		// Step-1
		RandomDataGenerator rdg = new  RandomDataGenerator();
		for(int i = 1; i <= n; i++) {
			double beta_i = rdg.nextBeta(0.5, (0.5 + i * 0.5)); // beta_i = Beta((1-d), (alpha + i*d))
			beta_map.put(i, beta_i);
			
			sum += beta_i;
			//System.out.println(i+" "+beta_i);			
		} 
		
		//System.out.println("@ beta_ sum = "+sum);
		//System.out.println("--------------------------------------");
		
		// Step-2
		sum = 0.0;
		pi_map.put(1, beta_map.get(1)); // pi_1 = beta_1
		//System.out.println("1 "+pi_map.get(1));
		
		for(int i = 2; i <= n; i++) {
			double _sum = 0.0;
			for(int l = 1; l <= i-1; l++) {
				_sum += (1- beta_map.get(l));
			}
			
			double pi_i = beta_map.get(i) * _sum;
			pi_map.put(i, pi_i);
			
			sum += pi_i;
			//System.out.println(i+" "+pi_i);
		}
		
		//System.out.println("@ pi_ sum = "+sum);
		//System.out.println("--------------------------------------");
		
		// Alternate
		for(Entry<Integer, Double> entry : beta_map.entrySet())
			System.out.println(entry.getValue()+"|"+pi_map.get(entry.getKey()));
		
		Map<Integer, ArrayList<Double>> _beta_map = new TreeMap<Integer, ArrayList<Double>>();
		BetaDistribution beta = new BetaDistribution(0.5, (0.5 + n * 0.5));
		for(int i = 1; i <= n; i++) {
			ArrayList<Double> values = new ArrayList<Double>();
			values.add(beta.probability(i));
			values.add(beta.cumulativeProbability(i));
			
			_beta_map.put(i, values);
		}
		
		pi_map = new TreeMap<Integer, Double>();
		sum = 0.0;
		pi_map.put(1, _beta_map.get(1).get(0)); // pi_1 = beta_1
		//System.out.println("1 "+pi_map.get(1));
		
		for(int i = 2; i <= n; i++) {
			double _sum = 0.0;
			for(int l = 1; l <= i-1; l++) {
				_sum += (1- _beta_map.get(l).get(0));
			}
			
			double pi_i = _beta_map.get(i).get(0) * _sum;
			pi_map.put(i, pi_i);
		}
		
		for(Entry<Integer, ArrayList<Double>> entry : _beta_map.entrySet())
			System.out.println(entry.getValue().get(0)+"|"+entry.getValue().get(1)+"|"+pi_map.get(entry.getKey()));
	}
}