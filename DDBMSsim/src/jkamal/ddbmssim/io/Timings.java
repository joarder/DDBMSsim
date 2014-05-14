package jkamal.ddbmssim.io;

public class Timings {
	private long start_time;
	
	public Timings(long start){
		this.setStart_time(start);
	}

	public long getStart_time() {
		return start_time;
	}

	public void setStart_time(long start_time) {
		this.start_time = start_time;
	}
}