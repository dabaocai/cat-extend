package com.zb.cat.task;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.zb.cat.domain.EsMapping;
import com.zb.cat.service.EsService;

/**
 * ClassName: EsInsertTaskThreadPool <br/>
 * Function: ES插入任务线程池 <br/>
 */
@Component
public class EsInsertTaskThreadPool {

    private static final Integer NUM_THREADS = 1;
    
	private static final int QUEUE_SIZE = 10000;
	private BlockingQueue<String> messageQueue = new LinkedBlockingQueue<String>(QUEUE_SIZE);

    @Autowired
    private EsService esService;
    
    private ExecutorService threadPool;
    
    private ConcurrentHashMap<String,EsMapping> mappings;
    
    public EsInsertTaskThreadPool() {
        threadPool = Executors.newFixedThreadPool(NUM_THREADS);
        mappings = new ConcurrentHashMap<String,EsMapping>();
    }
    
    @PostConstruct
    private void startConsuming() {
        consume();
    }
    
    private void consume() {
		threadPool.submit(new EsInsertTask(messageQueue, esService, mappings));
    }
    
    public void insert(String message){
        messageQueue.offer(message);
    }
}
