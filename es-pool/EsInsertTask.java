package com.zb.cat.task;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;

import com.google.gson.JsonObject;
import com.zb.cat.domain.EsIndex;
import com.zb.cat.domain.EsMapping;
import com.zb.cat.domain.EsProperty;
import com.zb.cat.service.EsService;
import com.zb.cat.util.JsonUtils;

/**
 * ClassName: EsInsertTask <br/>
 * Function: ES搜索引擎插入数据任务 <br/>
 */
public class EsInsertTask implements Runnable {

    private EsService esService;
    private BlockingQueue<String> queue;
    private ConcurrentHashMap<String, EsMapping> mappings;
    private static EsMapping defultMapping;

    private static int BATCH_SIZE = 100;

    static {
        EsMapping _defultMapping = readDefultMapping();
        if (_defultMapping != null) {
            defultMapping = _defultMapping;
        } else {
            throw new RuntimeException("Read defult mapping error!");
        }
    }

    public EsInsertTask(BlockingQueue<String> queue, EsService esService,
                        ConcurrentHashMap<String, EsMapping> mappings) {
        this.queue = queue;
        this.esService = esService;
        this.mappings = mappings;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void run() {
        // 内存缓冲区
        List<EsIndex> indexes = new ArrayList<EsIndex>(BATCH_SIZE);

        while (true) {
            try {
                // 批量100条一次插入ES
                if (indexes.size() == BATCH_SIZE) {
                    esService.builder(indexes);
                    indexes.clear();
                }

                // 消费数据
                String message = queue.poll();
                if (null != message) {
                    Map<String, Object> messages = JsonUtils.fromJson(message, HashMap.class);
                    String indexName = esService.getDefaultIndex();
                    String indexAlias = esService.getDefaultAlias();
                    String indexType = (String) messages.get("_domain");
                    String mappingKey = indexName + "|" + indexType;

                    if (mappings.get(mappingKey) == null) {
                        boolean indexExist = esService.isIndexExist(indexName);
                        if (!indexExist) {
                            boolean b = esService.createIndex(indexName, buildIndexSettings(indexType, indexAlias));
                            if (b) {
                                mappings.put(mappingKey, defultMapping);
                            }
                        } else {
                        	EsMapping mapping = esService.getMapping(indexName, indexType);
                        	if (mapping == null) {
                                boolean b = esService.createType(indexName, indexType, JsonUtils.toJson(defultMapping));
                                if (b) {
                                    mappings.put(mappingKey, defultMapping);
                                }
							}else {
								mappings.put(mappingKey, mapping);
							}
                        }
                    }

                    if (mappings.get(mappingKey) != null) {
                        Set<String> keySet = messages.keySet();
                        for (String key : keySet) {
                            // 收到的json中存在mapping里没有的字段
                            if (!mappings.get(mappingKey).getProperties().containsKey(key)) {
                                EsMapping mapping = new EsMapping();
                                Map<String, EsProperty> properties = mapping.getProperties();
                                EsProperty property = new EsProperty();
                                // 默认字段属性从配置文件中获取，非默认字段进行类型猜测
                                if (defultMapping.getProperties().containsKey(key)) {
                                    property = defultMapping.getProperties().get(key);
                                } else {
                                    property = toProperty(messages.get(key));
                                }
                                properties.put(key, property);
                                boolean b = esService.createType(indexName, indexType, JsonUtils.toJson(mapping));
                                if (b) {
                                    mappings.get(mappingKey).getProperties().put(key, property);
                                }
                            }
                        }
                        // 放入缓冲区
                        EsIndex index = new EsIndex();
                        index.setIndexName(indexName);
                        index.setIndexType(indexType);
                        index.setSource(message);
                        indexes.add(index);
                    }
                } else {
                    // 没收到数据，把缓冲区数据插入ES。
                    if (indexes.size() != 0) {
                        esService.builder(indexes);
                        indexes.clear();
                    }

                    Thread.sleep(500);
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    public static EsMapping readDefultMapping() {
        Properties prop = new Properties();
        InputStream in = EsInsertTask.class.getResourceAsStream("/esmapping.properties");
        if (in != null) {
            try {
                prop.load(in);
                // 创建ES映射
                EsMapping mapping = new EsMapping();
                Map<String, EsProperty> properties = mapping.getProperties();
                Set<Object> keySet = prop.keySet();
                for (Object key : keySet) {
                    String[] split = StringUtils.split((String) key, ".");
                    String propertiesKey = split[0];
                    String propertyName = split[1];
                    EsProperty property = properties.get(propertiesKey);
                    if (property == null) {
                        property = new EsProperty();
                    }
                    // 反射给属性对象设值
                    Field[] fields = property.getClass().getDeclaredFields();
                    for (Field field : fields) {
                        field.setAccessible(true); // 设置些属性是可以访问的
                        if (propertyName.equals(field.getName())) {
                            try {
                                field.set(property, prop.get(key));
                            } catch (IllegalArgumentException e) {
                                e.printStackTrace();
                            } catch (IllegalAccessException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                    properties.put(propertiesKey, property);
                }
                return mapping;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }
    
    private static String buildIndexSettings(String indexType, String indexAlias) {
        JsonObject _settings = new JsonObject();
        _settings.addProperty("number_of_shards", "6");
        _settings.addProperty("number_of_replicas", "0");

        JsonObject _aliases = new JsonObject();
        _aliases.add(indexAlias, new JsonObject());

        JsonObject _mappings = new JsonObject();
        _mappings.add(indexType, JsonUtils.toJsonObject(defultMapping));

        JsonObject root = new JsonObject();
        root.add("settings", _settings);
        root.add("aliases", _aliases);
        root.add("mappings", _mappings);
        return root.toString();
    }
    
    /**
     * toProperty:猜测字段类型并转换 <br/>
     *
     * @param object
     * @return
     */
    private static EsProperty toProperty(Object object) {
    	try {
    		Long.parseLong(object.toString());
    		EsProperty property = new EsProperty();
    		property.setType("long");
    		return property;
    	} catch (NumberFormatException e) {
    		try {
    			Double.parseDouble(object.toString());
    			EsProperty property = new EsProperty();
    			property.setType("double");
    			return property;
    		} catch (NumberFormatException nfe) {
    			try {
    				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    				sdf.parse(object.toString());
    				EsProperty property = new EsProperty();
    				property.setType("date");
    				property.setFormat("yyyy-MM-dd HH:mm:ss");
    				return property;
    			} catch (ParseException pe) {
    				EsProperty property = new EsProperty();
    				property.setType("keyword");
    				return property;
    			}
    		}
    	}
    }
    
}
