/**
 * @author Joarder Kamal
 */

package jkamal.ddbmssim.util;

import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.math3.distribution.BetaDistribution;
import org.apache.commons.math3.random.RandomDataGenerator;
 
public class PitmanYor {
	private double _d; // 0 <= d < 1
	private double _alpha; // Concentration parameter, alpha > -d
	
	public double get_d() {
		return _d;
	}

	public void set_d(double d) {
		this._d = d;
	}

	public double get_alpha() {
		return _alpha;
	}

	public void set_alpha(double alpha) {
		this._alpha = alpha;
	}

	public PitmanYor(double d, double alpha) {
		this.set_d(d);
		this.set_alpha(alpha);
	}
	
	private double scale(double x, double a, double b, double min, double max) {
		//System.out.println(x+"|"+a+"|"+b+"|"+min+"|"+max);
		
		return ((((b - a) * (x - min))/(max - min)) + a);
	}
	
	public static void main(String[] args) {
		// py(d, alpha) -- py(1/a, 0.5) where a = 2 and a is the power-law parameter (a > 1)
		int n = 100; // total number of data objects
		PitmanYor py = new PitmanYor(0.5, 0.5); // d = 0.5, alpha = 0.5		
				
		Map<Integer, ArrayList<Double>> betaMap = new TreeMap<Integer, ArrayList<Double>>();
		ArrayList<Double> betaList = null;		
		
		// Step-1
		RandomDataGenerator rdg = new  RandomDataGenerator();
		BetaDistribution betaDistribution = new BetaDistribution(0.5, 0.5);
		
		double beta_i = 0.0;
		double d = py.get_d();
		double alpha = py.get_alpha(); 
		
		for(int i = 1; i <= n; i++) {
			betaList = new ArrayList<Double>();

			beta_i = rdg.nextBeta((1 - d), (alpha + i * d)); // beta_i = Beta((1-d), (alpha + i*d))
			betaList.add(beta_i);
			betaList.add(betaDistribution.cumulativeProbability(((double)i/(double)n)));
			
			betaMap.put(i, betaList);			
		} 

		// Step-2
		Map<Integer, ArrayList<Double>> piMap = new TreeMap<Integer, ArrayList<Double>>();
		ArrayList<Double> piList = new ArrayList<Double>();
		double max = Integer.MIN_VALUE;
		double min = Integer.MAX_VALUE;
		
		// for i=1; pi_1 = beta_1 
		piList.add(betaMap.get(1).get(0));		
		piMap.put(1, piList); 		
		
		// for i = 2:n
		for(int i = 2; i <= n; i++) {
			double sum = 0.0;
			piList = new ArrayList<Double>();
			
			for(int l = 1; l <= i-1; l++) {
				sum += (1- betaMap.get(l).get(0));
			}
			
			double pi_i = betaMap.get(i).get(0) * sum;
			
			if(pi_i > max)
				max = pi_i;
			
			if(pi_i < min)
				min = pi_i;
			
			piList.add(pi_i);
			piMap.put(i, piList);
		}		
				
		System.out.println(">> "+max+" | "+min);
		
		// scale pi_i values between 0 to 1		
		for(Entry<Integer, ArrayList<Double>> entry : piMap.entrySet()) {
			double val = py.scale(entry.getValue().get(0), 0, 1, min, max);
			//System.out.println("@ "+val);
			entry.getValue().add(val);
		}
		
		// Print
		for(Entry<Integer, ArrayList<Double>> entry : betaMap.entrySet())
			System.out.println(
					entry.getKey()
					+"|"+entry.getValue().get(0)
					+"|"+entry.getValue().get(1)
					+"|"+piMap.get(entry.getKey()).get(0)
					+"|"+piMap.get(entry.getKey()).get(1));		
	}
}