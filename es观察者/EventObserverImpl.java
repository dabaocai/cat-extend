package com.dianping.cat.observer;

import org.springframework.beans.factory.annotation.Autowired;

import com.dianping.cat.consumer.event.EventAnalyzer;
import com.dianping.cat.consumer.event.IEventObserver;
import com.zb.cat.task.EsInsertTaskThreadPool;


public class EventObserverImpl implements IEventObserver {

	@Autowired
    private EsInsertTaskThreadPool esInsertTaskThreadPool;

    public void init(){
        EventAnalyzer.registerObserver("elasticSearchConsumer",this);
    }

    @Override
    public void addOrupdate(String json) {
        esInsertTaskThreadPool.insert(json);

    }
}
