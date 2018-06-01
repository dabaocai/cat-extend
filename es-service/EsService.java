package com.zb.cat.service;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import com.zb.cat.domain.EsIndex;
import com.zb.cat.domain.EsMapping;

import io.searchbox.core.BulkResult.BulkResultItem;


/**
 * ClassName: EsService <br/>
 * Function: ES搜索引擎服务 <br/>
 *
 */
public interface EsService {
	
	/**
	 * builder:批量创建索引插入数据 <br/>
	 *
	 * @param indexName
	 * @param indexType
	 * @param messages
	 * @return
	 */
	public List<BulkResultItem> builder(List<EsIndex> indexes) throws Exception;
	
	/**
	 * createIndex:创建索引 . <br/>
	 *
	 * @param indexName
	 * @param settings 为null时用默认值
	 * @return
	 */
	public boolean createIndex(String indexName, Object settings) throws Exception;
	
	/**
	 * createAlias:创建别名 <br/>
	 *
	 * @param indexName
	 * @param alias
	 * @return
	 */
	public boolean addAlias(String indexName, String alias) throws Exception;
	
	/**
	 * createType:手动创建类型(map一旦定义创建，field只能新增，不能修改) <br/>
	 *
	 * @param indexName
	 * @param indexType
	 * @param mappingString  <br/>
	 * 如{ "properties": { "id": { "type": "long", "store": "yes",
	 *            "index": "analyzed" }, "name": { "type": "string", "store":
	 *            "no", "index": "analyzed" } } }
	 * @return
	 */
	public boolean createType(String indexName, String indexType, String mappingString) throws Exception;
	
	/**
	 * getMapping:获取索引类型mapping <br/>
	 *
	 * @param indexName
	 * @param typeName
	 * @return
	 */
	public String getMappingString(String indexName, String typeName) throws Exception;
	
	/**
	 * getMappingProperties:获取索引类型mapping的属性值 <br/>
	 *
	 * @param indexName
	 * @param typeName
	 * @return
	 */
	public EsMapping getMapping(String indexName, String typeName) throws Exception;
	
	/**
	 * deleteDoc:删除文档 <br/>
	 *
	 * @param docId
	 * @param indexName
	 * @param indexType
	 * @return
	 */
	public boolean deleteDoc(String docId, String indexName, String indexType) throws Exception;
	
	/**
	 * deleteIndex:删除索引 <br/>
	 *
	 * @param indexName
	 * @return
	 */
	public boolean deleteIndex(String indexName) throws Exception;
	
	/**
	 * insertOrUpdateDoc:插入或更新文档 <br/>
	 *
	 * @param docId 为null时插入，不为null时更新
	 * @param docObject
	 * @param indexName
	 * @param indexType
	 * @return
	 * @throws IOException
	 */
	public boolean insertOrUpdateDoc(String docId, Object docObject, String indexName, String indexType) throws Exception;

	/**
	 * getSettingsString:获取settings. <br/>
	 *
	 * @param indexName
	 * @return
	 */
	public String getSettingsString(String indexName) throws Exception;

	/**
	 * isIndexExist:索引是否存在. <br/>
	 *
	 * @param indexName
	 * @return
	 */
	boolean isIndexExist(String indexName) throws Exception;
	
	public List<String> searchIndexByAlias(String alias) throws Exception;
	
	public String getDefaultIndex();
	
	public String getDefaultAlias();
	
	public Date getCreateDateByIndexName(String indexName) throws Exception;
}
