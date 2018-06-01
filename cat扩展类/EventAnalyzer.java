package com.dianping.cat.consumer.event;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.codehaus.plexus.logging.LogEnabled;
import org.codehaus.plexus.logging.Logger;
import org.unidal.lookup.annotation.Inject;

import com.dianping.cat.Constants;
import com.dianping.cat.analysis.AbstractMessageAnalyzer;
import com.dianping.cat.common.ContexKeys;
import com.dianping.cat.config.server.ServerFilterConfigManager;
import com.dianping.cat.consumer.event.model.entity.EventName;
import com.dianping.cat.consumer.event.model.entity.EventReport;
import com.dianping.cat.consumer.event.model.entity.EventType;
import com.dianping.cat.consumer.event.model.entity.Range;
import com.dianping.cat.message.Event;
import com.dianping.cat.message.Message;
import com.dianping.cat.message.Transaction;
import com.dianping.cat.message.internal.DefaultEvent;
import com.dianping.cat.message.internal.DefaultTransaction;
import com.dianping.cat.message.spi.MessageTree;
import com.dianping.cat.report.DefaultReportManager.StoragePolicy;
import com.dianping.cat.report.ReportManager;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class EventAnalyzer extends AbstractMessageAnalyzer<EventReport> implements LogEnabled {

	public static final String ID = "event";

	@Inject
	private EventDelegate m_delegate;

	@Inject(ID)
	private ReportManager<EventReport> m_reportManager;

	@Inject
	private ServerFilterConfigManager m_serverFilterConfigManager;

	private static ConcurrentHashMap<String, IEventObserver> concurrentHashMap = new ConcurrentHashMap<String, IEventObserver>();

	private static Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").create();

	private EventTpsStatisticsComputer m_computer = new EventTpsStatisticsComputer();

	public static void registerObserver(String key, IEventObserver iEventObserver) {
		concurrentHashMap.put(key, iEventObserver);
	}

	@Override
	public synchronized void doCheckpoint(boolean atEnd) {
		if (atEnd && !isLocalMode()) {
			m_reportManager.storeHourlyReports(getStartTime(), StoragePolicy.FILE_AND_DB, m_index);
		} else {
			m_reportManager.storeHourlyReports(getStartTime(), StoragePolicy.FILE, m_index);
		}
	}

	@Override
	public void enableLogging(Logger logger) {
		m_logger = logger;
	}

	@Override
	public int getAnanlyzerCount() {
		return 2;
	}

	@Override
	public EventReport getReport(String domain) {
		if (!Constants.ALL.equals(domain)) {
			long period = getStartTime();
			long timestamp = System.currentTimeMillis();
			long remainder = timestamp % 3600000;
			long current = timestamp - remainder;
			EventReport report = m_reportManager.getHourlyReport(period, domain, false);

			report.getDomainNames().addAll(m_reportManager.getDomains(getStartTime()));
			if (period == current) {
				report.accept(m_computer.setDuration(remainder / 1000));
			} else if (period < current) {
				report.accept(m_computer.setDuration(3600));
			}
			return report;
		} else {
			Map<String, EventReport> reports = m_reportManager.getHourlyReports(getStartTime());

			return m_delegate.createAggregatedReport(reports);
		}
	}

	@Override
	public ReportManager<EventReport> getReportManager() {
		return m_reportManager;
	}

	@Override
	protected void loadReports() {
		m_reportManager.loadHourlyReports(getStartTime(), StoragePolicy.FILE, m_index);
	}

	@Override
	public void process(MessageTree tree) {
		String domain = tree.getDomain();

		if (m_serverFilterConfigManager.validateDomain(domain)) {
			EventReport report = m_reportManager.getHourlyReport(getStartTime(), domain, true);
			Message message = tree.getMessage();
			String ip = tree.getIpAddress();

			if (message instanceof Transaction) {
				processTransaction(report, tree, (Transaction) message, ip);
				insertZbConsumerMessage(tree);
			} else if (message instanceof Event) {
				processEvent(report, tree, (Event) message, ip);
				insertZbConsumerMessage(tree);
			}
		}
	}

	private void processEvent(EventReport report, MessageTree tree, Event event, String ip) {
		int count = 1;
		EventType type = report.findOrCreateMachine(ip).findOrCreateType(event.getType());
		EventName name = type.findOrCreateName(event.getName());
		String messageId = tree.getMessageId();

		report.addIp(tree.getIpAddress());
		type.incTotalCount(count);
		name.incTotalCount(count);

		if (event.isSuccess()) {
			type.setSuccessMessageUrl(messageId);
			name.setSuccessMessageUrl(messageId);
		} else {
			type.incFailCount(count);
			name.incFailCount(count);

			type.setFailMessageUrl(messageId);
			name.setFailMessageUrl(messageId);
		}
		type.setFailPercent(type.getFailCount() * 100.0 / type.getTotalCount());
		name.setFailPercent(name.getFailCount() * 100.0 / name.getTotalCount());

		processEventGrpah(name, event, count);
	}

	private void processEventGrpah(EventName name, Event t, int count) {
		long current = t.getTimestamp() / 1000 / 60;
		int min = (int) (current % (60));
		Range range = name.findOrCreateRange(min);

		range.incCount(count);
		if (!t.isSuccess()) {
			range.incFails(count);
		}
	}

	private void processTransaction(EventReport report, MessageTree tree, Transaction t, String ip) {
		List<Message> children = t.getChildren();

		for (Message child : children) {
			if (child instanceof Transaction) {
				processTransaction(report, tree, (Transaction) child, ip);
			} else if (child instanceof Event) {
				processEvent(report, tree, (Event) child, ip);
			}
		}
	}

	public void setReportManager(ReportManager<EventReport> reportManager) {
		m_reportManager = reportManager;
	}

	private void insertZbConsumerMessage(MessageTree messageTree) {
		if (messageTree != null && messageTree.getMessage() != null) {
			List<Map<String, Object>> maps = new ArrayList<>();
			Map<String, Object> map = new HashMap<>();
			map.put("_domain", messageTree.getDomain());
			map.put("_ip", messageTree.getIpAddress());
			map.put("_threadId", messageTree.getThreadId());
			map.put("_preReqId",
					messageTree.getParentMessageId() != null && messageTree.getParentMessageId().length() != 4
							? messageTree.getParentMessageId() : messageTree.getMessageId());
			map.put("_rootReqId", messageTree.getRootMessageId() != null && messageTree.getRootMessageId().length() != 4
					? messageTree.getRootMessageId() : messageTree.getMessageId());
			map.put("_reqId", messageTree.getMessageId());
			defaultTransactionToMap(messageTree.getMessage(), maps, map);
			if (maps != null && maps.size() > 0) {
				Iterator<Entry<String, IEventObserver>> iterator = concurrentHashMap.entrySet().iterator();
				String dateNow = getDate();// 避免重复获取几乎不变的时间
				while (iterator.hasNext()) {
					Entry<String, IEventObserver> entry = iterator.next();
					IEventObserver iEventObserver = (IEventObserver) entry.getValue();
					for (Map<String, Object> m : maps) {
						if (m.containsKey("_env") && m.containsKey("_logType") && m.containsKey("_domain")
								&& m.containsKey("_eventType")) {
							m.put(ContexKeys.CREATE_TIME, dateNow);// 追加elasticsearch创建时间
							iEventObserver.addOrupdate(gson.toJson(m));
						}
					}
				}
			}
		}
	}

	private void defaultTransactionToMap(Message message, List<Map<String, Object>> maps,
			Map<String, Object> objectMap) {
		if (message == null)  return;
		Map<String, Object> map = new HashMap<>();
		map.putAll(objectMap);
		if (message instanceof DefaultTransaction) {
			map.put("_msg", message.getData());
			map.put("_msgName", message.getName());
			String json = message.getLogData();
			JsonParser parser = new JsonParser();
			JsonElement element = parser.parse(json);
			if (!element.isJsonNull() && element.isJsonObject()) {
				JsonObject jsonObj = element.getAsJsonObject();
				if (jsonObj != null && !jsonObj.isJsonNull()) {
					map.putAll(toMap(jsonObj));
				}
			}
			if (!element.isJsonNull() && element.isJsonArray()) {
				JsonArray jsonArray = element.getAsJsonArray();
				if (!jsonArray.isJsonNull() && jsonArray.size() > 0) {
					JsonObject jsonObj = jsonArray.getAsJsonObject();
					map.putAll(toMap(jsonObj));
				}
			}
			if(!map.containsKey(ContexKeys.COST)) {
				map.put(ContexKeys.COST, ((DefaultTransaction) message).getDurationInMillis());
			}

			List<Message> messages = ((DefaultTransaction) message).getChildren();
			if (messages != null && messages.size() > 0) {
				for (Message m : messages) {
					defaultTransactionToMap(m, maps, objectMap);
				}
			}
		} else if (message instanceof DefaultEvent) {
			Map<String, Object> eventMap = defaultEventToMap(message, objectMap);
			map.putAll(eventMap);
		}
		if (map.containsKey("_env") && map.containsKey("_logType") && map.containsKey("_domain")
				&& map.containsKey("_eventType")) {
			maps.add(map);
		}
	}

	private Map<String, Object> defaultEventToMap(Message message, Map<String, Object> map) {
		Map<String, Object> eventMap = new HashMap<>();
		eventMap.putAll(map);
		if (message != null) {
			eventMap.put("_msg", message.getData());
			eventMap.put("_msgName", message.getName());
			String json = message.getLogData();
			JsonParser parser = new JsonParser();
			JsonElement jsonElement = parser.parse(json);
			if (jsonElement != null && !jsonElement.isJsonNull() && jsonElement.isJsonObject()) {
				JsonObject jsonObj = parser.parse(json).getAsJsonObject();
				if (jsonObj != null && !jsonObj.isJsonNull()) {
					eventMap.putAll(toMap(jsonObj));
				}
			} else if (jsonElement != null && !jsonElement.isJsonNull() && jsonElement.isJsonArray()) {
				JsonArray jsonArray = parser.parse(json).getAsJsonArray();
				if (jsonArray != null && !jsonArray.isJsonNull() && jsonArray.size() > 0) {
					JsonObject jsonObj = jsonArray.getAsJsonObject();
					eventMap.putAll(toMap(jsonObj));
				}
			}
		}
		return eventMap;
	}

	private static String getDate() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return sdf.format(new Date());
	}

	/**
	 * 将JSONObjec对象转换成Map-List集合
	 * 
	 * @param json
	 * @return
	 */
	public static Map<String, Object> toMap(JsonObject json) {
		Map<String, Object> map = new HashMap<String, Object>();
		if (json != null) {
			Set<Map.Entry<String, JsonElement>> entrySet = json.entrySet();
			for (Iterator<Map.Entry<String, JsonElement>> iter = entrySet.iterator(); iter.hasNext();) {
				Map.Entry<String, JsonElement> entry = iter.next();
				String key = entry.getKey();
				Object value = entry.getValue();
				if (value instanceof JsonArray) {
					map.put(key, toList((JsonArray) value));
				} else if (value instanceof JsonObject) {
					map.put(key, toMap((JsonObject) value));
				} else {
					map.put(key, value);
				}

			}
		}
		return map;
	}

	/**
	 * 将JSONArray对象转换成List集合
	 * 
	 * @param json
	 * @return
	 */
	public static List<Object> toList(JsonArray json) {
		List<Object> list = new ArrayList<Object>();
		for (int i = 0; i < json.size(); i++) {
			Object value = json.get(i);
			if (value instanceof JsonArray) {
				list.add(toList((JsonArray) value));
			} else if (value instanceof JsonObject) {
				list.add(toMap((JsonObject) value));
			} else {
				list.add(value);
			}
		}
		return list;
	}
}
