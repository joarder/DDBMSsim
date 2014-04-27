package jkamal.ddbmssim.incmine.learners;

import moa.core.TimingUtils;
import jkamal.ddbmssim.incmine.streams.ZakiFileStream;

public class Main {
    
    public static void main(String args[]){
        //ZakiFileStream stream = new ZakiFileStream("C:\\merge-script\\stream1_stream2_drift-o0.25-l0.001.data");
        //ZakiFileStream stream = new ZakiFileStream("C:\\cygwin\\home\\Massimo\\n1000t15i10p6.data");
        //LEDGenerator stream = new LEDGenerator();
        ZakiFileStream stream = new ZakiFileStream("C:\\Users\\Joarder Kamal\\git\\DDBMSsim\\DDBMSsim\\lib\\native\\moa\\T40I10D100K.ascii");
        
        IncMine learner = new IncMine();
        learner.minSupportOption.setValue(0.03d);
        learner.relaxationRateOption.setValue(0.5d);
        learner.fixedSegmentLengthOption.setValue(1000);
        learner.windowSizeOption.setValue(10);
        learner.resetLearning();
        
        
        stream.prepareForUse();
        TimingUtils.enablePreciseTiming();
        long start = TimingUtils.getNanoCPUTimeOfCurrentThread();
        while(stream.hasMoreInstances()){
            learner.trainOnInstance(stream.nextInstance());
            
        }
        
        long end = TimingUtils.getNanoCPUTimeOfCurrentThread();
        double tp = 1e5/ ((double)(end - start) / 1e9);
        
        System.out.println(tp + "trans/sec");
    }
    
}
