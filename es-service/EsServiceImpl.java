package com.zb.cat.service.impl;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.zb.cat.domain.EsIndex;
import com.zb.cat.domain.EsMapping;
import com.zb.cat.domain.EsProperty;
import com.zb.cat.service.EsService;
import com.zb.cat.util.JsonUtils;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Bulk;
import io.searchbox.core.BulkResult;
import io.searchbox.core.BulkResult.BulkResultItem;
import io.searchbox.core.Cat;
import io.searchbox.core.CatResult;
import io.searchbox.core.Delete;
import io.searchbox.core.Index;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.IndicesExists;
import io.searchbox.indices.aliases.AddAliasMapping;
import io.searchbox.indices.aliases.ModifyAliases;
import io.searchbox.indices.mapping.GetMapping;
import io.searchbox.indices.mapping.PutMapping;
import io.searchbox.indices.settings.GetSettings;

/**
 * ClassName: EsServiceImpl <br/>
 * Function: ES搜索引擎服务实现类 <br/>
 *
 */
@Service
public class EsServiceImpl implements EsService {

	private static final Logger logger = LoggerFactory.getLogger(EsServiceImpl.class);

	@Autowired
	private JestClient jestClient;
	
	@Value("${com.zb.cat.env:dev}")
	private String env;

	@Override
	public String getDefaultIndex() {
		return env + "-" + new SimpleDateFormat("yyyyMMdd").format(new Date());
	}
	
	@Override
	public String getDefaultAlias() {
		return env;
	}
	
	@Override
	public Date getCreateDateByIndexName(String indexName) throws Exception {
		int begin = indexName.lastIndexOf("-");
		String substring = indexName.substring(begin + 1, indexName.length());
		Date createDate = new SimpleDateFormat("yyyyMMdd").parse(substring);
		return createDate;
	}
	
	@Override
	public List<BulkResultItem> builder(List<EsIndex> indexes) throws Exception {
		Bulk.Builder bulkBuilder = new Bulk.Builder();
		// 添加数据
		for (EsIndex index : indexes) {
			bulkBuilder.addAction(new Index.Builder(index.getSource()).index(index.getIndexName()).type(index.getIndexType()).build());
		}
		long start = System.currentTimeMillis();
		BulkResult result = jestClient.execute(bulkBuilder.build());
		long end = System.currentTimeMillis();
		logger.info("elasticsearch批量插入{}条: -->> {}毫秒", indexes.size() , (end - start) );
		List<BulkResultItem> failedItems = result.getFailedItems();
		if (CollectionUtils.isNotEmpty(failedItems)) {
			for (BulkResultItem bulkResultItem : failedItems) {
				logger.warn("错误信息: -->> " + bulkResultItem.error);
			}
		}
		return result.getItems();
	}
	
	@Override
	public boolean createIndex(String indexName, Object settings) throws Exception {
		CreateIndex createIndex = null;
		if (settings != null) {
			createIndex = new CreateIndex.Builder(indexName).settings(settings).build();
		}else {
			createIndex = new CreateIndex.Builder(indexName).build();
		}
		JestResult result = jestClient.execute(createIndex);
		if (result != null && result.isSucceeded()) {
			return true;
		}
		logger.warn("创建索引失败:"+ result.getErrorMessage());
		return false;
	}
	
	@Override
	public boolean addAlias(String indexName, String alias) throws Exception{
		AddAliasMapping addAliasMapping = new AddAliasMapping.Builder(indexName, alias).build();
		ModifyAliases modifyAliases = new ModifyAliases.Builder(addAliasMapping).build();
		JestResult result = jestClient.execute(modifyAliases);
		if (result != null && result.isSucceeded()) {
			return true;
		}
		logger.warn("添加别名失败:"+ result.getErrorMessage());
		return false;
	}

	@Override
	public boolean createType(String indexName, String indexType, String mappingString) throws Exception{
		PutMapping.Builder builder = new PutMapping.Builder(indexName, indexType, mappingString);
		JestResult result = jestClient.execute(builder.build());
		if (result != null && result.isSucceeded()) {
			return true;
		}
		logger.warn("创建索引类型失败:"+ result.getErrorMessage());
		return false;
	}

	@Override
	public String getMappingString(String indexName, String typeName) throws Exception{
		GetMapping.Builder builder = new GetMapping.Builder();
		builder.addIndex(indexName).addType(typeName);
		JestResult result = jestClient.execute(builder.build());
		if (result != null && result.isSucceeded()) {
			return result.getSourceAsObject(JsonObject.class).toString();
		}
		return null;
	}
	
	@Override
	public String getSettingsString(String indexName) throws Exception{
		GetSettings.Builder builder = new GetSettings.Builder();
		builder.addIndex(indexName);
		JestResult result = jestClient.execute(builder.build());
		if (result != null && result.isSucceeded()) {
			return result.getSourceAsObject(JsonObject.class).toString();
		}
		return null;
	}
	
	@Override
	public boolean isIndexExist(String indexName) throws Exception{
    	IndicesExists.Builder builder = new IndicesExists.Builder(indexName);
		JestResult result = jestClient.execute(builder.build());
        if (result != null && result.isSucceeded()) {
            return true;
        }
        return false;
	}
	
	@Override
	public EsMapping getMapping(String indexName, String typeName) throws Exception{
		GetMapping.Builder builder = new GetMapping.Builder();
		builder.addIndex(indexName).addType(typeName);
		JestResult result = jestClient.execute(builder.build());
		if (result != null && result.isSucceeded()) {
	        JsonObject jsonResult = result.getJsonObject();
	        if (jsonResult != null && jsonResult.size() > 0) {
	            JsonObject jsonObject = jsonResult.getAsJsonObject(indexName).getAsJsonObject("mappings").getAsJsonObject(typeName).getAsJsonObject("properties");
	            if (jsonObject != null) {
	                Set<Entry<String, JsonElement>> set = jsonObject.entrySet();
	                if (set != null && set.size() > 0) {
	                	Map<String, EsProperty> properties = new HashMap<String, EsProperty>();
	                    for (Map.Entry<String, JsonElement> jsonElementEntry : set) {
	                    	properties.put(jsonElementEntry.getKey(), JsonUtils.fromJson(jsonElementEntry.getValue().toString(), EsProperty.class));
	                    }
	                    if (properties.size() > 0) {
	                    	EsMapping mapping = new EsMapping();
	                    	mapping.setProperties(properties);
	                    	return mapping;
	                    }
	                }
	            }
	        }
		}
		return null;
	}

	@Override
	public boolean deleteDoc(String docId, String indexName, String indexType) throws Exception{
		Delete.Builder builder = new Delete.Builder(docId);
		Delete delete = builder.index(indexName).type(indexType).build();
		JestResult result = jestClient.execute(delete);
		if (result != null && result.isSucceeded()) {
			return true;
		}
		logger.warn("删除文档失败:"+ result.getErrorMessage());
		return false;
	}

	@Override
	public boolean deleteIndex(String indexName) throws Exception{
		DeleteIndex delete = new DeleteIndex.Builder(indexName).build();
		JestResult result = jestClient.execute(delete);
		if (result != null && result.isSucceeded()) {
			return true;
		}
		logger.warn("删除索引失败:"+ result.getErrorMessage());
		return false;
	}

	@Override
	public boolean insertOrUpdateDoc(String docId, Object docObject, String indexName, String indexType) throws Exception{
		Index.Builder builder = new Index.Builder(docObject);
		builder.id(docId);
		Index index = builder.index(indexName).type(indexType).build();
		long start = System.currentTimeMillis();
		JestResult result = jestClient.execute(index);
		if (result != null && result.isSucceeded()) {
			long end = System.currentTimeMillis();
			logger.info("插入更新文档时间: -->> " + (end - start) + " 毫秒");
			return true;
		}
		logger.warn("插入更新文档失败:"+ result.getErrorMessage());
		return false;
	}
	
	public List<String> searchIndexByAlias(String alias) throws Exception {
        Cat cat = new Cat.AliasesBuilder().addIndex(alias).build();
        CatResult result = jestClient.execute(cat);
        if (result != null && result.isSucceeded()) {
        	JsonObject jsonObject = result.getJsonObject();
        	List<String> indexNames = new ArrayList<String>();
        	if (jsonObject != null && jsonObject.get("result") != null && jsonObject.get("result").isJsonArray()) {
        		JsonArray asJsonArray = jsonObject.get("result").getAsJsonArray();
        		for (JsonElement jsonElement : asJsonArray) {
        			JsonElement element = jsonElement.getAsJsonObject().get("index");
        			if (element != null ) {
        				indexNames.add(element.getAsString());
        			}
        		}
        	}
        	return indexNames;
		}
        return null;
	}
}
