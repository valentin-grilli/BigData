package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import pojo.Category;
import conditions.CategoryAttribute;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.ArrayList;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.commons.lang.mutable.MutableBoolean;
import conditions.Condition;
import conditions.Operator;
import util.Util;

public abstract class CategoryService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CategoryService.class);
	


	public static enum ROLE_NAME {
		
	}
	private static java.util.Map<ROLE_NAME, loading.Loading> defaultLoadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	static {
	}
	
	private java.util.Map<ROLE_NAME, loading.Loading> loadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	
	public CategoryService() {
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
			loadingParameters.put(entry.getKey(), entry.getValue());
	}
	
	public CategoryService(java.util.Map<ROLE_NAME, loading.Loading> loadingParams) {
		this();
		if(loadingParams != null)
			for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: loadingParams.entrySet())
				loadingParameters.put(entry.getKey(), entry.getValue());
	}
	
	public static java.util.Map<ROLE_NAME, loading.Loading> getDefaultLoadingParameters() {
		java.util.Map<ROLE_NAME, loading.Loading> res = new java.util.HashMap<ROLE_NAME, loading.Loading>();
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
				res.put(entry.getKey(), entry.getValue());
		return res;
	}
	
	public static void setAllDefaultLoadingParameters(loading.Loading loading) {
		java.util.Map<ROLE_NAME, loading.Loading> newParams = new java.util.HashMap<ROLE_NAME, loading.Loading>();
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
				newParams.put(entry.getKey(), entry.getValue());
		defaultLoadingParameters = newParams;
	}
	
	public java.util.Map<ROLE_NAME, loading.Loading> getLoadingParameters() {
		return this.loadingParameters;
	}
	
	public void setLoadingParameters(java.util.Map<ROLE_NAME, loading.Loading> newParams) {
		this.loadingParameters = newParams;
	}
	
	public void updateLoadingParameter(ROLE_NAME role, loading.Loading l) {
		this.loadingParameters.put(role, l);
	}
	
	
	public Dataset<Category> getCategoryList(){
		return getCategoryList(null);
	}
	
	public Dataset<Category> getCategoryList(conditions.Condition<conditions.CategoryAttribute> condition){
		MutableBoolean refilterFlag = new MutableBoolean(false);
		List<Dataset<Category>> datasets = new ArrayList<Dataset<Category>>();
		Dataset<Category> d = null;
		d = getCategoryListInCategoryPairsFromRedisDB(condition, refilterFlag);
		if(d != null)
			datasets.add(d);
		
		if(datasets.size() == 0)
			return null;
	
		d = datasets.get(0);
		if(datasets.size() > 1) {
			d=fullOuterJoinsCategory(datasets);
		}
		if(refilterFlag.booleanValue())
			d = d.filter((FilterFunction<Category>) r -> condition == null || condition.evaluate(r));
		d = d.dropDuplicates(new String[] {"id"});
		return d;
	}
	
	
	
	
	
	public abstract Dataset<Category> getCategoryListInCategoryPairsFromRedisDB(conditions.Condition<conditions.CategoryAttribute> condition, MutableBoolean refilterFlag);
	
	
	public Category getCategoryById(Integer id){
		Condition cond;
		cond = Condition.simple(CategoryAttribute.id, conditions.Operator.EQUALS, id);
		Dataset<Category> res = getCategoryList(cond);
		if(res!=null && !res.isEmpty())
			return res.first();
		return null;
	}
	
	public Dataset<Category> getCategoryListById(Integer id) {
		return getCategoryList(conditions.Condition.simple(conditions.CategoryAttribute.id, conditions.Operator.EQUALS, id));
	}
	
	public Dataset<Category> getCategoryListByCategoryName(String categoryName) {
		return getCategoryList(conditions.Condition.simple(conditions.CategoryAttribute.categoryName, conditions.Operator.EQUALS, categoryName));
	}
	
	public Dataset<Category> getCategoryListByDescription(String description) {
		return getCategoryList(conditions.Condition.simple(conditions.CategoryAttribute.description, conditions.Operator.EQUALS, description));
	}
	
	public Dataset<Category> getCategoryListByPicture(String picture) {
		return getCategoryList(conditions.Condition.simple(conditions.CategoryAttribute.picture, conditions.Operator.EQUALS, picture));
	}
	
	
	
	public static Dataset<Category> fullOuterJoinsCategory(List<Dataset<Category>> datasetsPOJO) {
		return fullOuterJoinsCategory(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Category> fullLeftOuterJoinsCategory(List<Dataset<Category>> datasetsPOJO) {
		return fullOuterJoinsCategory(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Category> fullOuterJoinsCategory(List<Dataset<Category>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		Dataset<Category> d = datasetsPOJO.get(0);
			List<String> idFields = new ArrayList<String>();
			idFields.add("id");
			logger.debug("Start {} of [{}] datasets of [Category] objects",joinMode,datasetsPOJO.size());
			scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
			Dataset<Row> res = d.join(datasetsPOJO.get(1)
								.withColumnRenamed("categoryName", "categoryName_1")
								.withColumnRenamed("description", "description_1")
								.withColumnRenamed("picture", "picture_1")
								.withColumnRenamed("logEvents", "logEvents_1")
							, seq, joinMode);
			for(int i = 2; i < datasetsPOJO.size(); i++) {
				res = res.join(datasetsPOJO.get(i)
								.withColumnRenamed("categoryName", "categoryName_" + i)
								.withColumnRenamed("description", "description_" + i)
								.withColumnRenamed("picture", "picture_" + i)
								.withColumnRenamed("logEvents", "logEvents_" + i)
						, seq, joinMode);
			}
			logger.debug("End join. Start");
			logger.debug("Start transforming Row objects to [Category] objects"); 
			d = res.map((MapFunction<Row, Category>) r -> {
					Category category_res = new Category();
					
					// attribute 'Category.id'
					Integer firstNotNull_id = Util.getIntegerValue(r.getAs("id"));
					category_res.setId(firstNotNull_id);
					
					// attribute 'Category.categoryName'
					String firstNotNull_categoryName = Util.getStringValue(r.getAs("categoryName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String categoryName2 = Util.getStringValue(r.getAs("categoryName_" + i));
						if (firstNotNull_categoryName != null && categoryName2 != null && !firstNotNull_categoryName.equals(categoryName2)) {
							category_res.addLogEvent("Data consistency problem for [Category - id :"+category_res.getId()+"]: different values found for attribute 'Category.categoryName': " + firstNotNull_categoryName + " and " + categoryName2 + "." );
							logger.warn("Data consistency problem for [Category - id :"+category_res.getId()+"]: different values found for attribute 'Category.categoryName': " + firstNotNull_categoryName + " and " + categoryName2 + "." );
						}
						if (firstNotNull_categoryName == null && categoryName2 != null) {
							firstNotNull_categoryName = categoryName2;
						}
					}
					category_res.setCategoryName(firstNotNull_categoryName);
					
					// attribute 'Category.description'
					String firstNotNull_description = Util.getStringValue(r.getAs("description"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String description2 = Util.getStringValue(r.getAs("description_" + i));
						if (firstNotNull_description != null && description2 != null && !firstNotNull_description.equals(description2)) {
							category_res.addLogEvent("Data consistency problem for [Category - id :"+category_res.getId()+"]: different values found for attribute 'Category.description': " + firstNotNull_description + " and " + description2 + "." );
							logger.warn("Data consistency problem for [Category - id :"+category_res.getId()+"]: different values found for attribute 'Category.description': " + firstNotNull_description + " and " + description2 + "." );
						}
						if (firstNotNull_description == null && description2 != null) {
							firstNotNull_description = description2;
						}
					}
					category_res.setDescription(firstNotNull_description);
					
					// attribute 'Category.picture'
					String firstNotNull_picture = Util.getStringValue(r.getAs("picture"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String picture2 = Util.getStringValue(r.getAs("picture_" + i));
						if (firstNotNull_picture != null && picture2 != null && !firstNotNull_picture.equals(picture2)) {
							category_res.addLogEvent("Data consistency problem for [Category - id :"+category_res.getId()+"]: different values found for attribute 'Category.picture': " + firstNotNull_picture + " and " + picture2 + "." );
							logger.warn("Data consistency problem for [Category - id :"+category_res.getId()+"]: different values found for attribute 'Category.picture': " + firstNotNull_picture + " and " + picture2 + "." );
						}
						if (firstNotNull_picture == null && picture2 != null) {
							firstNotNull_picture = picture2;
						}
					}
					category_res.setPicture(firstNotNull_picture);
	
					WrappedArray logEvents = r.getAs("logEvents");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							category_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							category_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					return category_res;
				}, Encoders.bean(Category.class));
			return d;
	}
	
	
	
	
	
	
	
	
	public abstract boolean insertCategory(Category category);
	
	public abstract boolean insertCategoryInCategoryPairsFromRedisDB(Category category); 
	private boolean inUpdateMethod = false;
	private List<Row> allCategoryIdList = null;
	public abstract void updateCategoryList(conditions.Condition<conditions.CategoryAttribute> condition, conditions.SetClause<conditions.CategoryAttribute> set);
	
	public void updateCategory(pojo.Category category) {
		//TODO using the id
		return;
	}
	
	
	public abstract void deleteCategoryList(conditions.Condition<conditions.CategoryAttribute> condition);
	
	public void deleteCategory(pojo.Category category) {
		//TODO using the id
		return;
	}
	
}
