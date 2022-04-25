package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import conditions.*;
import pojo.Belongs_to;
import java.time.LocalDate;
import java.time.LocalDateTime;
import tdo.*;
import pojo.*;
import org.apache.commons.lang.mutable.MutableBoolean;
import java.util.List;
import java.util.ArrayList;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Encoders;
import org.apache.spark.api.java.function.MapFunction;
import util.Util;


public abstract class Belongs_toService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Belongs_toService.class);
	
	
	// Left side 'CategoryRef' of reference [categoryR ]
	public abstract Dataset<ProductTDO> getProductTDOListProductInCategoryRInProductsInfoFromRelSchema(Condition<ProductAttribute> condition, MutableBoolean refilterFlag);
	
	// Right side 'categoryid' of reference [categoryR ]
	public abstract Dataset<CategoryTDO> getCategoryTDOListCategoryInCategoryRInProductsInfoFromRelSchema(Condition<CategoryAttribute> condition, MutableBoolean refilterFlag);
	
	
	
	
	public static Dataset<Belongs_to> fullLeftOuterJoinBetweenBelongs_toAndProduct(Dataset<Belongs_to> d1, Dataset<Product> d2) {
		Dataset<Row> d2_ = d2
			.withColumnRenamed("id", "A_id")
			.withColumnRenamed("name", "A_name")
			.withColumnRenamed("supplierRef", "A_supplierRef")
			.withColumnRenamed("categoryRef", "A_categoryRef")
			.withColumnRenamed("quantityPerUnit", "A_quantityPerUnit")
			.withColumnRenamed("unitPrice", "A_unitPrice")
			.withColumnRenamed("reorderLevel", "A_reorderLevel")
			.withColumnRenamed("discontinued", "A_discontinued")
			.withColumnRenamed("unitsInStock", "A_unitsInStock")
			.withColumnRenamed("unitsOnOrder", "A_unitsOnOrder")
			.withColumnRenamed("logEvents", "A_logEvents");
		
		Column joinCond = null;
		joinCond = d1.col("product.id").equalTo(d2_.col("A_id"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map((MapFunction<Row, Belongs_to>) r -> {
				Belongs_to res = new Belongs_to();
	
				Product product = new Product();
				Object o = r.getAs("product");
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						product.setId(Util.getIntegerValue(r2.getAs("id")));
						product.setName(Util.getStringValue(r2.getAs("name")));
						product.setSupplierRef(Util.getIntegerValue(r2.getAs("supplierRef")));
						product.setCategoryRef(Util.getIntegerValue(r2.getAs("categoryRef")));
						product.setQuantityPerUnit(Util.getStringValue(r2.getAs("quantityPerUnit")));
						product.setUnitPrice(Util.getDoubleValue(r2.getAs("unitPrice")));
						product.setReorderLevel(Util.getIntegerValue(r2.getAs("reorderLevel")));
						product.setDiscontinued(Util.getBooleanValue(r2.getAs("discontinued")));
						product.setUnitsInStock(Util.getIntegerValue(r2.getAs("unitsInStock")));
						product.setUnitsOnOrder(Util.getIntegerValue(r2.getAs("unitsOnOrder")));
					} 
					if(o instanceof Product) {
						product = (Product) o;
					}
				}
	
				res.setProduct(product);
	
				Integer id = Util.getIntegerValue(r.getAs("A_id"));
				if (product.getId() != null && id != null && !product.getId().equals(id)) {
					res.addLogEvent("Data consistency problem for [Belongs_to - different values found for attribute 'Belongs_to.id': " + product.getId() + " and " + id + "." );
					logger.warn("Data consistency problem for [Belongs_to - different values found for attribute 'Belongs_to.id': " + product.getId() + " and " + id + "." );
				}
				if(id != null)
					product.setId(id);
				String name = Util.getStringValue(r.getAs("A_name"));
				if (product.getName() != null && name != null && !product.getName().equals(name)) {
					res.addLogEvent("Data consistency problem for [Belongs_to - different values found for attribute 'Belongs_to.name': " + product.getName() + " and " + name + "." );
					logger.warn("Data consistency problem for [Belongs_to - different values found for attribute 'Belongs_to.name': " + product.getName() + " and " + name + "." );
				}
				if(name != null)
					product.setName(name);
				Integer supplierRef = Util.getIntegerValue(r.getAs("A_supplierRef"));
				if (product.getSupplierRef() != null && supplierRef != null && !product.getSupplierRef().equals(supplierRef)) {
					res.addLogEvent("Data consistency problem for [Belongs_to - different values found for attribute 'Belongs_to.supplierRef': " + product.getSupplierRef() + " and " + supplierRef + "." );
					logger.warn("Data consistency problem for [Belongs_to - different values found for attribute 'Belongs_to.supplierRef': " + product.getSupplierRef() + " and " + supplierRef + "." );
				}
				if(supplierRef != null)
					product.setSupplierRef(supplierRef);
				Integer categoryRef = Util.getIntegerValue(r.getAs("A_categoryRef"));
				if (product.getCategoryRef() != null && categoryRef != null && !product.getCategoryRef().equals(categoryRef)) {
					res.addLogEvent("Data consistency problem for [Belongs_to - different values found for attribute 'Belongs_to.categoryRef': " + product.getCategoryRef() + " and " + categoryRef + "." );
					logger.warn("Data consistency problem for [Belongs_to - different values found for attribute 'Belongs_to.categoryRef': " + product.getCategoryRef() + " and " + categoryRef + "." );
				}
				if(categoryRef != null)
					product.setCategoryRef(categoryRef);
				String quantityPerUnit = Util.getStringValue(r.getAs("A_quantityPerUnit"));
				if (product.getQuantityPerUnit() != null && quantityPerUnit != null && !product.getQuantityPerUnit().equals(quantityPerUnit)) {
					res.addLogEvent("Data consistency problem for [Belongs_to - different values found for attribute 'Belongs_to.quantityPerUnit': " + product.getQuantityPerUnit() + " and " + quantityPerUnit + "." );
					logger.warn("Data consistency problem for [Belongs_to - different values found for attribute 'Belongs_to.quantityPerUnit': " + product.getQuantityPerUnit() + " and " + quantityPerUnit + "." );
				}
				if(quantityPerUnit != null)
					product.setQuantityPerUnit(quantityPerUnit);
				Double unitPrice = Util.getDoubleValue(r.getAs("A_unitPrice"));
				if (product.getUnitPrice() != null && unitPrice != null && !product.getUnitPrice().equals(unitPrice)) {
					res.addLogEvent("Data consistency problem for [Belongs_to - different values found for attribute 'Belongs_to.unitPrice': " + product.getUnitPrice() + " and " + unitPrice + "." );
					logger.warn("Data consistency problem for [Belongs_to - different values found for attribute 'Belongs_to.unitPrice': " + product.getUnitPrice() + " and " + unitPrice + "." );
				}
				if(unitPrice != null)
					product.setUnitPrice(unitPrice);
				Integer reorderLevel = Util.getIntegerValue(r.getAs("A_reorderLevel"));
				if (product.getReorderLevel() != null && reorderLevel != null && !product.getReorderLevel().equals(reorderLevel)) {
					res.addLogEvent("Data consistency problem for [Belongs_to - different values found for attribute 'Belongs_to.reorderLevel': " + product.getReorderLevel() + " and " + reorderLevel + "." );
					logger.warn("Data consistency problem for [Belongs_to - different values found for attribute 'Belongs_to.reorderLevel': " + product.getReorderLevel() + " and " + reorderLevel + "." );
				}
				if(reorderLevel != null)
					product.setReorderLevel(reorderLevel);
				Boolean discontinued = Util.getBooleanValue(r.getAs("A_discontinued"));
				if (product.getDiscontinued() != null && discontinued != null && !product.getDiscontinued().equals(discontinued)) {
					res.addLogEvent("Data consistency problem for [Belongs_to - different values found for attribute 'Belongs_to.discontinued': " + product.getDiscontinued() + " and " + discontinued + "." );
					logger.warn("Data consistency problem for [Belongs_to - different values found for attribute 'Belongs_to.discontinued': " + product.getDiscontinued() + " and " + discontinued + "." );
				}
				if(discontinued != null)
					product.setDiscontinued(discontinued);
				Integer unitsInStock = Util.getIntegerValue(r.getAs("A_unitsInStock"));
				if (product.getUnitsInStock() != null && unitsInStock != null && !product.getUnitsInStock().equals(unitsInStock)) {
					res.addLogEvent("Data consistency problem for [Belongs_to - different values found for attribute 'Belongs_to.unitsInStock': " + product.getUnitsInStock() + " and " + unitsInStock + "." );
					logger.warn("Data consistency problem for [Belongs_to - different values found for attribute 'Belongs_to.unitsInStock': " + product.getUnitsInStock() + " and " + unitsInStock + "." );
				}
				if(unitsInStock != null)
					product.setUnitsInStock(unitsInStock);
				Integer unitsOnOrder = Util.getIntegerValue(r.getAs("A_unitsOnOrder"));
				if (product.getUnitsOnOrder() != null && unitsOnOrder != null && !product.getUnitsOnOrder().equals(unitsOnOrder)) {
					res.addLogEvent("Data consistency problem for [Belongs_to - different values found for attribute 'Belongs_to.unitsOnOrder': " + product.getUnitsOnOrder() + " and " + unitsOnOrder + "." );
					logger.warn("Data consistency problem for [Belongs_to - different values found for attribute 'Belongs_to.unitsOnOrder': " + product.getUnitsOnOrder() + " and " + unitsOnOrder + "." );
				}
				if(unitsOnOrder != null)
					product.setUnitsOnOrder(unitsOnOrder);
	
				o = r.getAs("category");
				Category category = new Category();
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						category.setId(Util.getIntegerValue(r2.getAs("id")));
						category.setCategoryName(Util.getStringValue(r2.getAs("categoryName")));
						category.setDescription(Util.getStringValue(r2.getAs("description")));
						category.setPicture(Util.getStringValue(r2.getAs("picture")));
					} 
					if(o instanceof Category) {
						category = (Category) o;
					}
				}
	
				res.setCategory(category);
	
				return res;
		}, Encoders.bean(Belongs_to.class));
	
		
		
	}
	public static Dataset<Belongs_to> fullLeftOuterJoinBetweenBelongs_toAndCategory(Dataset<Belongs_to> d1, Dataset<Category> d2) {
		Dataset<Row> d2_ = d2
			.withColumnRenamed("id", "A_id")
			.withColumnRenamed("categoryName", "A_categoryName")
			.withColumnRenamed("description", "A_description")
			.withColumnRenamed("picture", "A_picture")
			.withColumnRenamed("logEvents", "A_logEvents");
		
		Column joinCond = null;
		joinCond = d1.col("category.id").equalTo(d2_.col("A_id"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map((MapFunction<Row, Belongs_to>) r -> {
				Belongs_to res = new Belongs_to();
	
				Category category = new Category();
				Object o = r.getAs("category");
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						category.setId(Util.getIntegerValue(r2.getAs("id")));
						category.setCategoryName(Util.getStringValue(r2.getAs("categoryName")));
						category.setDescription(Util.getStringValue(r2.getAs("description")));
						category.setPicture(Util.getStringValue(r2.getAs("picture")));
					} 
					if(o instanceof Category) {
						category = (Category) o;
					}
				}
	
				res.setCategory(category);
	
				Integer id = Util.getIntegerValue(r.getAs("A_id"));
				if (category.getId() != null && id != null && !category.getId().equals(id)) {
					res.addLogEvent("Data consistency problem for [Belongs_to - different values found for attribute 'Belongs_to.id': " + category.getId() + " and " + id + "." );
					logger.warn("Data consistency problem for [Belongs_to - different values found for attribute 'Belongs_to.id': " + category.getId() + " and " + id + "." );
				}
				if(id != null)
					category.setId(id);
				String categoryName = Util.getStringValue(r.getAs("A_categoryName"));
				if (category.getCategoryName() != null && categoryName != null && !category.getCategoryName().equals(categoryName)) {
					res.addLogEvent("Data consistency problem for [Belongs_to - different values found for attribute 'Belongs_to.categoryName': " + category.getCategoryName() + " and " + categoryName + "." );
					logger.warn("Data consistency problem for [Belongs_to - different values found for attribute 'Belongs_to.categoryName': " + category.getCategoryName() + " and " + categoryName + "." );
				}
				if(categoryName != null)
					category.setCategoryName(categoryName);
				String description = Util.getStringValue(r.getAs("A_description"));
				if (category.getDescription() != null && description != null && !category.getDescription().equals(description)) {
					res.addLogEvent("Data consistency problem for [Belongs_to - different values found for attribute 'Belongs_to.description': " + category.getDescription() + " and " + description + "." );
					logger.warn("Data consistency problem for [Belongs_to - different values found for attribute 'Belongs_to.description': " + category.getDescription() + " and " + description + "." );
				}
				if(description != null)
					category.setDescription(description);
				String picture = Util.getStringValue(r.getAs("A_picture"));
				if (category.getPicture() != null && picture != null && !category.getPicture().equals(picture)) {
					res.addLogEvent("Data consistency problem for [Belongs_to - different values found for attribute 'Belongs_to.picture': " + category.getPicture() + " and " + picture + "." );
					logger.warn("Data consistency problem for [Belongs_to - different values found for attribute 'Belongs_to.picture': " + category.getPicture() + " and " + picture + "." );
				}
				if(picture != null)
					category.setPicture(picture);
	
				o = r.getAs("product");
				Product product = new Product();
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						product.setId(Util.getIntegerValue(r2.getAs("id")));
						product.setName(Util.getStringValue(r2.getAs("name")));
						product.setSupplierRef(Util.getIntegerValue(r2.getAs("supplierRef")));
						product.setCategoryRef(Util.getIntegerValue(r2.getAs("categoryRef")));
						product.setQuantityPerUnit(Util.getStringValue(r2.getAs("quantityPerUnit")));
						product.setUnitPrice(Util.getDoubleValue(r2.getAs("unitPrice")));
						product.setReorderLevel(Util.getIntegerValue(r2.getAs("reorderLevel")));
						product.setDiscontinued(Util.getBooleanValue(r2.getAs("discontinued")));
						product.setUnitsInStock(Util.getIntegerValue(r2.getAs("unitsInStock")));
						product.setUnitsOnOrder(Util.getIntegerValue(r2.getAs("unitsOnOrder")));
					} 
					if(o instanceof Product) {
						product = (Product) o;
					}
				}
	
				res.setProduct(product);
	
				return res;
		}, Encoders.bean(Belongs_to.class));
	
		
		
	}
	
	public static Dataset<Belongs_to> fullOuterJoinsBelongs_to(List<Dataset<Belongs_to>> datasetsPOJO) {
		return fullOuterJoinsBelongs_to(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Belongs_to> fullLeftOuterJoinsBelongs_to(List<Dataset<Belongs_to>> datasetsPOJO) {
		return fullOuterJoinsBelongs_to(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Belongs_to> fullOuterJoinsBelongs_to(List<Dataset<Belongs_to>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		List<String> idFields = new ArrayList<String>();
		idFields.add("product.id");
	
		idFields.add("category.id");
		scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
		
		List<Dataset<Row>> rows = new ArrayList<Dataset<Row>>();
		for(int i = 0; i < datasetsPOJO.size(); i++) {
			Dataset<Belongs_to> d = datasetsPOJO.get(i);
			rows.add(d
				.withColumn("product_id_" + i, d.col("product.id"))
				.withColumn("category_id_" + i, d.col("category.id"))
				.withColumnRenamed("product", "product_" + i)
				.withColumnRenamed("category", "category_" + i)
				.withColumnRenamed("logEvents", "logEvents_" + i));
		}
		
		Column joinCond;
		joinCond = rows.get(0).col("product_id_0").equalTo(rows.get(1).col("product_id_1"));
		joinCond = joinCond.and(rows.get(0).col("category_id_0").equalTo(rows.get(1).col("category_id_1")));
		
		Dataset<Row> res = rows.get(0).join(rows.get(1), joinCond, joinMode);
		for(int i = 2; i < rows.size(); i++) {
			joinCond = rows.get(i - 1).col("product_id_" + (i - 1)).equalTo(rows.get(i).col("product_id_" + i));
			joinCond = joinCond.and(rows.get(i - 1).col("category_id_" + (i - 1)).equalTo(rows.get(i).col("category_id_" + i)));
			res = res.join(rows.get(i), joinCond, joinMode);
		}
	
		return res.map((MapFunction<Row, Belongs_to>) r -> {
				Belongs_to belongs_to_res = new Belongs_to();
	
					WrappedArray logEvents = r.getAs("logEvents_0");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							belongs_to_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							belongs_to_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					Product product_res = new Product();
					Category category_res = new Category();
					
					// attribute 'Product.id'
					Integer firstNotNull_product_id = Util.getIntegerValue(r.getAs("product_0.id"));
					product_res.setId(firstNotNull_product_id);
					// attribute 'Product.name'
					String firstNotNull_product_name = Util.getStringValue(r.getAs("product_0.name"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String product_name2 = Util.getStringValue(r.getAs("product_" + i + ".name"));
						if (firstNotNull_product_name != null && product_name2 != null && !firstNotNull_product_name.equals(product_name2)) {
							belongs_to_res.addLogEvent("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.name': " + firstNotNull_product_name + " and " + product_name2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.name': " + firstNotNull_product_name + " and " + product_name2 + "." );
						}
						if (firstNotNull_product_name == null && product_name2 != null) {
							firstNotNull_product_name = product_name2;
						}
					}
					product_res.setName(firstNotNull_product_name);
					// attribute 'Product.supplierRef'
					Integer firstNotNull_product_supplierRef = Util.getIntegerValue(r.getAs("product_0.supplierRef"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer product_supplierRef2 = Util.getIntegerValue(r.getAs("product_" + i + ".supplierRef"));
						if (firstNotNull_product_supplierRef != null && product_supplierRef2 != null && !firstNotNull_product_supplierRef.equals(product_supplierRef2)) {
							belongs_to_res.addLogEvent("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.supplierRef': " + firstNotNull_product_supplierRef + " and " + product_supplierRef2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.supplierRef': " + firstNotNull_product_supplierRef + " and " + product_supplierRef2 + "." );
						}
						if (firstNotNull_product_supplierRef == null && product_supplierRef2 != null) {
							firstNotNull_product_supplierRef = product_supplierRef2;
						}
					}
					product_res.setSupplierRef(firstNotNull_product_supplierRef);
					// attribute 'Product.categoryRef'
					Integer firstNotNull_product_categoryRef = Util.getIntegerValue(r.getAs("product_0.categoryRef"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer product_categoryRef2 = Util.getIntegerValue(r.getAs("product_" + i + ".categoryRef"));
						if (firstNotNull_product_categoryRef != null && product_categoryRef2 != null && !firstNotNull_product_categoryRef.equals(product_categoryRef2)) {
							belongs_to_res.addLogEvent("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.categoryRef': " + firstNotNull_product_categoryRef + " and " + product_categoryRef2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.categoryRef': " + firstNotNull_product_categoryRef + " and " + product_categoryRef2 + "." );
						}
						if (firstNotNull_product_categoryRef == null && product_categoryRef2 != null) {
							firstNotNull_product_categoryRef = product_categoryRef2;
						}
					}
					product_res.setCategoryRef(firstNotNull_product_categoryRef);
					// attribute 'Product.quantityPerUnit'
					String firstNotNull_product_quantityPerUnit = Util.getStringValue(r.getAs("product_0.quantityPerUnit"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String product_quantityPerUnit2 = Util.getStringValue(r.getAs("product_" + i + ".quantityPerUnit"));
						if (firstNotNull_product_quantityPerUnit != null && product_quantityPerUnit2 != null && !firstNotNull_product_quantityPerUnit.equals(product_quantityPerUnit2)) {
							belongs_to_res.addLogEvent("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.quantityPerUnit': " + firstNotNull_product_quantityPerUnit + " and " + product_quantityPerUnit2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.quantityPerUnit': " + firstNotNull_product_quantityPerUnit + " and " + product_quantityPerUnit2 + "." );
						}
						if (firstNotNull_product_quantityPerUnit == null && product_quantityPerUnit2 != null) {
							firstNotNull_product_quantityPerUnit = product_quantityPerUnit2;
						}
					}
					product_res.setQuantityPerUnit(firstNotNull_product_quantityPerUnit);
					// attribute 'Product.unitPrice'
					Double firstNotNull_product_unitPrice = Util.getDoubleValue(r.getAs("product_0.unitPrice"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Double product_unitPrice2 = Util.getDoubleValue(r.getAs("product_" + i + ".unitPrice"));
						if (firstNotNull_product_unitPrice != null && product_unitPrice2 != null && !firstNotNull_product_unitPrice.equals(product_unitPrice2)) {
							belongs_to_res.addLogEvent("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.unitPrice': " + firstNotNull_product_unitPrice + " and " + product_unitPrice2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.unitPrice': " + firstNotNull_product_unitPrice + " and " + product_unitPrice2 + "." );
						}
						if (firstNotNull_product_unitPrice == null && product_unitPrice2 != null) {
							firstNotNull_product_unitPrice = product_unitPrice2;
						}
					}
					product_res.setUnitPrice(firstNotNull_product_unitPrice);
					// attribute 'Product.reorderLevel'
					Integer firstNotNull_product_reorderLevel = Util.getIntegerValue(r.getAs("product_0.reorderLevel"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer product_reorderLevel2 = Util.getIntegerValue(r.getAs("product_" + i + ".reorderLevel"));
						if (firstNotNull_product_reorderLevel != null && product_reorderLevel2 != null && !firstNotNull_product_reorderLevel.equals(product_reorderLevel2)) {
							belongs_to_res.addLogEvent("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.reorderLevel': " + firstNotNull_product_reorderLevel + " and " + product_reorderLevel2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.reorderLevel': " + firstNotNull_product_reorderLevel + " and " + product_reorderLevel2 + "." );
						}
						if (firstNotNull_product_reorderLevel == null && product_reorderLevel2 != null) {
							firstNotNull_product_reorderLevel = product_reorderLevel2;
						}
					}
					product_res.setReorderLevel(firstNotNull_product_reorderLevel);
					// attribute 'Product.discontinued'
					Boolean firstNotNull_product_discontinued = Util.getBooleanValue(r.getAs("product_0.discontinued"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Boolean product_discontinued2 = Util.getBooleanValue(r.getAs("product_" + i + ".discontinued"));
						if (firstNotNull_product_discontinued != null && product_discontinued2 != null && !firstNotNull_product_discontinued.equals(product_discontinued2)) {
							belongs_to_res.addLogEvent("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.discontinued': " + firstNotNull_product_discontinued + " and " + product_discontinued2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.discontinued': " + firstNotNull_product_discontinued + " and " + product_discontinued2 + "." );
						}
						if (firstNotNull_product_discontinued == null && product_discontinued2 != null) {
							firstNotNull_product_discontinued = product_discontinued2;
						}
					}
					product_res.setDiscontinued(firstNotNull_product_discontinued);
					// attribute 'Product.unitsInStock'
					Integer firstNotNull_product_unitsInStock = Util.getIntegerValue(r.getAs("product_0.unitsInStock"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer product_unitsInStock2 = Util.getIntegerValue(r.getAs("product_" + i + ".unitsInStock"));
						if (firstNotNull_product_unitsInStock != null && product_unitsInStock2 != null && !firstNotNull_product_unitsInStock.equals(product_unitsInStock2)) {
							belongs_to_res.addLogEvent("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.unitsInStock': " + firstNotNull_product_unitsInStock + " and " + product_unitsInStock2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.unitsInStock': " + firstNotNull_product_unitsInStock + " and " + product_unitsInStock2 + "." );
						}
						if (firstNotNull_product_unitsInStock == null && product_unitsInStock2 != null) {
							firstNotNull_product_unitsInStock = product_unitsInStock2;
						}
					}
					product_res.setUnitsInStock(firstNotNull_product_unitsInStock);
					// attribute 'Product.unitsOnOrder'
					Integer firstNotNull_product_unitsOnOrder = Util.getIntegerValue(r.getAs("product_0.unitsOnOrder"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer product_unitsOnOrder2 = Util.getIntegerValue(r.getAs("product_" + i + ".unitsOnOrder"));
						if (firstNotNull_product_unitsOnOrder != null && product_unitsOnOrder2 != null && !firstNotNull_product_unitsOnOrder.equals(product_unitsOnOrder2)) {
							belongs_to_res.addLogEvent("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.unitsOnOrder': " + firstNotNull_product_unitsOnOrder + " and " + product_unitsOnOrder2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.unitsOnOrder': " + firstNotNull_product_unitsOnOrder + " and " + product_unitsOnOrder2 + "." );
						}
						if (firstNotNull_product_unitsOnOrder == null && product_unitsOnOrder2 != null) {
							firstNotNull_product_unitsOnOrder = product_unitsOnOrder2;
						}
					}
					product_res.setUnitsOnOrder(firstNotNull_product_unitsOnOrder);
					// attribute 'Category.id'
					Integer firstNotNull_category_id = Util.getIntegerValue(r.getAs("category_0.id"));
					category_res.setId(firstNotNull_category_id);
					// attribute 'Category.categoryName'
					String firstNotNull_category_categoryName = Util.getStringValue(r.getAs("category_0.categoryName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String category_categoryName2 = Util.getStringValue(r.getAs("category_" + i + ".categoryName"));
						if (firstNotNull_category_categoryName != null && category_categoryName2 != null && !firstNotNull_category_categoryName.equals(category_categoryName2)) {
							belongs_to_res.addLogEvent("Data consistency problem for [Category - id :"+category_res.getId()+"]: different values found for attribute 'Category.categoryName': " + firstNotNull_category_categoryName + " and " + category_categoryName2 + "." );
							logger.warn("Data consistency problem for [Category - id :"+category_res.getId()+"]: different values found for attribute 'Category.categoryName': " + firstNotNull_category_categoryName + " and " + category_categoryName2 + "." );
						}
						if (firstNotNull_category_categoryName == null && category_categoryName2 != null) {
							firstNotNull_category_categoryName = category_categoryName2;
						}
					}
					category_res.setCategoryName(firstNotNull_category_categoryName);
					// attribute 'Category.description'
					String firstNotNull_category_description = Util.getStringValue(r.getAs("category_0.description"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String category_description2 = Util.getStringValue(r.getAs("category_" + i + ".description"));
						if (firstNotNull_category_description != null && category_description2 != null && !firstNotNull_category_description.equals(category_description2)) {
							belongs_to_res.addLogEvent("Data consistency problem for [Category - id :"+category_res.getId()+"]: different values found for attribute 'Category.description': " + firstNotNull_category_description + " and " + category_description2 + "." );
							logger.warn("Data consistency problem for [Category - id :"+category_res.getId()+"]: different values found for attribute 'Category.description': " + firstNotNull_category_description + " and " + category_description2 + "." );
						}
						if (firstNotNull_category_description == null && category_description2 != null) {
							firstNotNull_category_description = category_description2;
						}
					}
					category_res.setDescription(firstNotNull_category_description);
					// attribute 'Category.picture'
					String firstNotNull_category_picture = Util.getStringValue(r.getAs("category_0.picture"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String category_picture2 = Util.getStringValue(r.getAs("category_" + i + ".picture"));
						if (firstNotNull_category_picture != null && category_picture2 != null && !firstNotNull_category_picture.equals(category_picture2)) {
							belongs_to_res.addLogEvent("Data consistency problem for [Category - id :"+category_res.getId()+"]: different values found for attribute 'Category.picture': " + firstNotNull_category_picture + " and " + category_picture2 + "." );
							logger.warn("Data consistency problem for [Category - id :"+category_res.getId()+"]: different values found for attribute 'Category.picture': " + firstNotNull_category_picture + " and " + category_picture2 + "." );
						}
						if (firstNotNull_category_picture == null && category_picture2 != null) {
							firstNotNull_category_picture = category_picture2;
						}
					}
					category_res.setPicture(firstNotNull_category_picture);
	
					belongs_to_res.setProduct(product_res);
					belongs_to_res.setCategory(category_res);
					return belongs_to_res;
		}
		, Encoders.bean(Belongs_to.class));
	
	}
	
	//Empty arguments
	public Dataset<Belongs_to> getBelongs_toList(){
		 return getBelongs_toList(null,null);
	}
	
	public abstract Dataset<Belongs_to> getBelongs_toList(
		Condition<ProductAttribute> product_condition,
		Condition<CategoryAttribute> category_condition);
	
	public Dataset<Belongs_to> getBelongs_toListByProductCondition(
		Condition<ProductAttribute> product_condition
	){
		return getBelongs_toList(product_condition, null);
	}
	
	public Belongs_to getBelongs_toByProduct(Product product) {
		Condition<ProductAttribute> cond = null;
		cond = Condition.simple(ProductAttribute.id, Operator.EQUALS, product.getId());
		Dataset<Belongs_to> res = getBelongs_toListByProductCondition(cond);
		List<Belongs_to> list = res.collectAsList();
		if(list.size() > 0)
			return list.get(0);
		else
			return null;
	}
	public Dataset<Belongs_to> getBelongs_toListByCategoryCondition(
		Condition<CategoryAttribute> category_condition
	){
		return getBelongs_toList(null, category_condition);
	}
	
	public Dataset<Belongs_to> getBelongs_toListByCategory(Category category) {
		Condition<CategoryAttribute> cond = null;
		cond = Condition.simple(CategoryAttribute.id, Operator.EQUALS, category.getId());
		Dataset<Belongs_to> res = getBelongs_toListByCategoryCondition(cond);
	return res;
	}
	
	
	
	public abstract void deleteBelongs_toList(
		conditions.Condition<conditions.ProductAttribute> product_condition,
		conditions.Condition<conditions.CategoryAttribute> category_condition);
	
	public void deleteBelongs_toListByProductCondition(
		conditions.Condition<conditions.ProductAttribute> product_condition
	){
		deleteBelongs_toList(product_condition, null);
	}
	
	public void deleteBelongs_toByProduct(pojo.Product product) {
		// TODO using id for selecting
		return;
	}
	public void deleteBelongs_toListByCategoryCondition(
		conditions.Condition<conditions.CategoryAttribute> category_condition
	){
		deleteBelongs_toList(null, category_condition);
	}
	
	public void deleteBelongs_toListByCategory(pojo.Category category) {
		// TODO using id for selecting
		return;
	}
		
}
