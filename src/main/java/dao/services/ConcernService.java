package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import conditions.*;
import pojo.Concern;
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


public abstract class ConcernService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ConcernService.class);
	
	
	// Left side 'productid' of reference [concerned ]
	public abstract Dataset<StockInfoTDO> getStockInfoTDOListStockInConcernedInStockInfoPairsFromKv(Condition<StockInfoAttribute> condition, MutableBoolean refilterFlag);
	
	// Right side 'ProductID' of reference [concerned ]
	public abstract Dataset<ProductInfoTDO> getProductInfoTDOListProductInConcernedInStockInfoPairsFromKv(Condition<ProductInfoAttribute> condition, MutableBoolean refilterFlag);
	
	
	
	
	public static Dataset<Concern> fullLeftOuterJoinBetweenConcernAndStock(Dataset<Concern> d1, Dataset<StockInfo> d2) {
		Dataset<Row> d2_ = d2
			.withColumnRenamed("id", "A_id")
			.withColumnRenamed("unitsInStock", "A_unitsInStock")
			.withColumnRenamed("unitsOnOrder", "A_unitsOnOrder")
			.withColumnRenamed("logEvents", "A_logEvents");
		
		Column joinCond = null;
		joinCond = d1.col("stock.id").equalTo(d2_.col("A_id"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map((MapFunction<Row, Concern>) r -> {
				Concern res = new Concern();
	
				StockInfo stock = new StockInfo();
				Object o = r.getAs("stock");
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						stock.setId(Util.getIntegerValue(r2.getAs("id")));
						stock.setUnitsInStock(Util.getIntegerValue(r2.getAs("unitsInStock")));
						stock.setUnitsOnOrder(Util.getIntegerValue(r2.getAs("unitsOnOrder")));
					} 
					if(o instanceof StockInfo) {
						stock = (StockInfo) o;
					}
				}
	
				res.setStock(stock);
	
				Integer id = Util.getIntegerValue(r.getAs("A_id"));
				if (stock.getId() != null && id != null && !stock.getId().equals(id)) {
					res.addLogEvent("Data consistency problem for [Concern - different values found for attribute 'Concern.id': " + stock.getId() + " and " + id + "." );
					logger.warn("Data consistency problem for [Concern - different values found for attribute 'Concern.id': " + stock.getId() + " and " + id + "." );
				}
				if(id != null)
					stock.setId(id);
				Integer unitsInStock = Util.getIntegerValue(r.getAs("A_unitsInStock"));
				if (stock.getUnitsInStock() != null && unitsInStock != null && !stock.getUnitsInStock().equals(unitsInStock)) {
					res.addLogEvent("Data consistency problem for [Concern - different values found for attribute 'Concern.unitsInStock': " + stock.getUnitsInStock() + " and " + unitsInStock + "." );
					logger.warn("Data consistency problem for [Concern - different values found for attribute 'Concern.unitsInStock': " + stock.getUnitsInStock() + " and " + unitsInStock + "." );
				}
				if(unitsInStock != null)
					stock.setUnitsInStock(unitsInStock);
				Integer unitsOnOrder = Util.getIntegerValue(r.getAs("A_unitsOnOrder"));
				if (stock.getUnitsOnOrder() != null && unitsOnOrder != null && !stock.getUnitsOnOrder().equals(unitsOnOrder)) {
					res.addLogEvent("Data consistency problem for [Concern - different values found for attribute 'Concern.unitsOnOrder': " + stock.getUnitsOnOrder() + " and " + unitsOnOrder + "." );
					logger.warn("Data consistency problem for [Concern - different values found for attribute 'Concern.unitsOnOrder': " + stock.getUnitsOnOrder() + " and " + unitsOnOrder + "." );
				}
				if(unitsOnOrder != null)
					stock.setUnitsOnOrder(unitsOnOrder);
	
				o = r.getAs("product");
				ProductInfo product = new ProductInfo();
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
					} 
					if(o instanceof ProductInfo) {
						product = (ProductInfo) o;
					}
				}
	
				res.setProduct(product);
	
				return res;
		}, Encoders.bean(Concern.class));
	
		
		
	}
	public static Dataset<Concern> fullLeftOuterJoinBetweenConcernAndProduct(Dataset<Concern> d1, Dataset<ProductInfo> d2) {
		Dataset<Row> d2_ = d2
			.withColumnRenamed("id", "A_id")
			.withColumnRenamed("name", "A_name")
			.withColumnRenamed("supplierRef", "A_supplierRef")
			.withColumnRenamed("categoryRef", "A_categoryRef")
			.withColumnRenamed("quantityPerUnit", "A_quantityPerUnit")
			.withColumnRenamed("unitPrice", "A_unitPrice")
			.withColumnRenamed("reorderLevel", "A_reorderLevel")
			.withColumnRenamed("discontinued", "A_discontinued")
			.withColumnRenamed("logEvents", "A_logEvents");
		
		Column joinCond = null;
		joinCond = d1.col("product.id").equalTo(d2_.col("A_id"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map((MapFunction<Row, Concern>) r -> {
				Concern res = new Concern();
	
				ProductInfo product = new ProductInfo();
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
					} 
					if(o instanceof ProductInfo) {
						product = (ProductInfo) o;
					}
				}
	
				res.setProduct(product);
	
				Integer id = Util.getIntegerValue(r.getAs("A_id"));
				if (product.getId() != null && id != null && !product.getId().equals(id)) {
					res.addLogEvent("Data consistency problem for [Concern - different values found for attribute 'Concern.id': " + product.getId() + " and " + id + "." );
					logger.warn("Data consistency problem for [Concern - different values found for attribute 'Concern.id': " + product.getId() + " and " + id + "." );
				}
				if(id != null)
					product.setId(id);
				String name = Util.getStringValue(r.getAs("A_name"));
				if (product.getName() != null && name != null && !product.getName().equals(name)) {
					res.addLogEvent("Data consistency problem for [Concern - different values found for attribute 'Concern.name': " + product.getName() + " and " + name + "." );
					logger.warn("Data consistency problem for [Concern - different values found for attribute 'Concern.name': " + product.getName() + " and " + name + "." );
				}
				if(name != null)
					product.setName(name);
				Integer supplierRef = Util.getIntegerValue(r.getAs("A_supplierRef"));
				if (product.getSupplierRef() != null && supplierRef != null && !product.getSupplierRef().equals(supplierRef)) {
					res.addLogEvent("Data consistency problem for [Concern - different values found for attribute 'Concern.supplierRef': " + product.getSupplierRef() + " and " + supplierRef + "." );
					logger.warn("Data consistency problem for [Concern - different values found for attribute 'Concern.supplierRef': " + product.getSupplierRef() + " and " + supplierRef + "." );
				}
				if(supplierRef != null)
					product.setSupplierRef(supplierRef);
				Integer categoryRef = Util.getIntegerValue(r.getAs("A_categoryRef"));
				if (product.getCategoryRef() != null && categoryRef != null && !product.getCategoryRef().equals(categoryRef)) {
					res.addLogEvent("Data consistency problem for [Concern - different values found for attribute 'Concern.categoryRef': " + product.getCategoryRef() + " and " + categoryRef + "." );
					logger.warn("Data consistency problem for [Concern - different values found for attribute 'Concern.categoryRef': " + product.getCategoryRef() + " and " + categoryRef + "." );
				}
				if(categoryRef != null)
					product.setCategoryRef(categoryRef);
				String quantityPerUnit = Util.getStringValue(r.getAs("A_quantityPerUnit"));
				if (product.getQuantityPerUnit() != null && quantityPerUnit != null && !product.getQuantityPerUnit().equals(quantityPerUnit)) {
					res.addLogEvent("Data consistency problem for [Concern - different values found for attribute 'Concern.quantityPerUnit': " + product.getQuantityPerUnit() + " and " + quantityPerUnit + "." );
					logger.warn("Data consistency problem for [Concern - different values found for attribute 'Concern.quantityPerUnit': " + product.getQuantityPerUnit() + " and " + quantityPerUnit + "." );
				}
				if(quantityPerUnit != null)
					product.setQuantityPerUnit(quantityPerUnit);
				Double unitPrice = Util.getDoubleValue(r.getAs("A_unitPrice"));
				if (product.getUnitPrice() != null && unitPrice != null && !product.getUnitPrice().equals(unitPrice)) {
					res.addLogEvent("Data consistency problem for [Concern - different values found for attribute 'Concern.unitPrice': " + product.getUnitPrice() + " and " + unitPrice + "." );
					logger.warn("Data consistency problem for [Concern - different values found for attribute 'Concern.unitPrice': " + product.getUnitPrice() + " and " + unitPrice + "." );
				}
				if(unitPrice != null)
					product.setUnitPrice(unitPrice);
				Integer reorderLevel = Util.getIntegerValue(r.getAs("A_reorderLevel"));
				if (product.getReorderLevel() != null && reorderLevel != null && !product.getReorderLevel().equals(reorderLevel)) {
					res.addLogEvent("Data consistency problem for [Concern - different values found for attribute 'Concern.reorderLevel': " + product.getReorderLevel() + " and " + reorderLevel + "." );
					logger.warn("Data consistency problem for [Concern - different values found for attribute 'Concern.reorderLevel': " + product.getReorderLevel() + " and " + reorderLevel + "." );
				}
				if(reorderLevel != null)
					product.setReorderLevel(reorderLevel);
				Boolean discontinued = Util.getBooleanValue(r.getAs("A_discontinued"));
				if (product.getDiscontinued() != null && discontinued != null && !product.getDiscontinued().equals(discontinued)) {
					res.addLogEvent("Data consistency problem for [Concern - different values found for attribute 'Concern.discontinued': " + product.getDiscontinued() + " and " + discontinued + "." );
					logger.warn("Data consistency problem for [Concern - different values found for attribute 'Concern.discontinued': " + product.getDiscontinued() + " and " + discontinued + "." );
				}
				if(discontinued != null)
					product.setDiscontinued(discontinued);
	
				o = r.getAs("stock");
				StockInfo stock = new StockInfo();
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						stock.setId(Util.getIntegerValue(r2.getAs("id")));
						stock.setUnitsInStock(Util.getIntegerValue(r2.getAs("unitsInStock")));
						stock.setUnitsOnOrder(Util.getIntegerValue(r2.getAs("unitsOnOrder")));
					} 
					if(o instanceof StockInfo) {
						stock = (StockInfo) o;
					}
				}
	
				res.setStock(stock);
	
				return res;
		}, Encoders.bean(Concern.class));
	
		
		
	}
	
	public static Dataset<Concern> fullOuterJoinsConcern(List<Dataset<Concern>> datasetsPOJO) {
		return fullOuterJoinsConcern(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Concern> fullLeftOuterJoinsConcern(List<Dataset<Concern>> datasetsPOJO) {
		return fullOuterJoinsConcern(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Concern> fullOuterJoinsConcern(List<Dataset<Concern>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		List<String> idFields = new ArrayList<String>();
		idFields.add("stock.id");
	
		idFields.add("product.id");
		scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
		
		List<Dataset<Row>> rows = new ArrayList<Dataset<Row>>();
		for(int i = 0; i < datasetsPOJO.size(); i++) {
			Dataset<Concern> d = datasetsPOJO.get(i);
			rows.add(d
				.withColumn("stock_id_" + i, d.col("stock.id"))
				.withColumn("product_id_" + i, d.col("product.id"))
				.withColumnRenamed("stock", "stock_" + i)
				.withColumnRenamed("product", "product_" + i)
				.withColumnRenamed("logEvents", "logEvents_" + i));
		}
		
		Column joinCond;
		joinCond = rows.get(0).col("stock_id_0").equalTo(rows.get(1).col("stock_id_1"));
		joinCond = joinCond.and(rows.get(0).col("product_id_0").equalTo(rows.get(1).col("product_id_1")));
		
		Dataset<Row> res = rows.get(0).join(rows.get(1), joinCond, joinMode);
		for(int i = 2; i < rows.size(); i++) {
			joinCond = rows.get(i - 1).col("stock_id_" + (i - 1)).equalTo(rows.get(i).col("stock_id_" + i));
			joinCond = joinCond.and(rows.get(i - 1).col("product_id_" + (i - 1)).equalTo(rows.get(i).col("product_id_" + i)));
			res = res.join(rows.get(i), joinCond, joinMode);
		}
	
		return res.map((MapFunction<Row, Concern>) r -> {
				Concern concern_res = new Concern();
	
					WrappedArray logEvents = r.getAs("logEvents_0");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							concern_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							concern_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					StockInfo stock_res = new StockInfo();
					ProductInfo product_res = new ProductInfo();
					
					// attribute 'StockInfo.id'
					Integer firstNotNull_stock_id = Util.getIntegerValue(r.getAs("stock_0.id"));
					stock_res.setId(firstNotNull_stock_id);
					// attribute 'StockInfo.unitsInStock'
					Integer firstNotNull_stock_unitsInStock = Util.getIntegerValue(r.getAs("stock_0.unitsInStock"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer stock_unitsInStock2 = Util.getIntegerValue(r.getAs("stock_" + i + ".unitsInStock"));
						if (firstNotNull_stock_unitsInStock != null && stock_unitsInStock2 != null && !firstNotNull_stock_unitsInStock.equals(stock_unitsInStock2)) {
							concern_res.addLogEvent("Data consistency problem for [StockInfo - id :"+stock_res.getId()+"]: different values found for attribute 'StockInfo.unitsInStock': " + firstNotNull_stock_unitsInStock + " and " + stock_unitsInStock2 + "." );
							logger.warn("Data consistency problem for [StockInfo - id :"+stock_res.getId()+"]: different values found for attribute 'StockInfo.unitsInStock': " + firstNotNull_stock_unitsInStock + " and " + stock_unitsInStock2 + "." );
						}
						if (firstNotNull_stock_unitsInStock == null && stock_unitsInStock2 != null) {
							firstNotNull_stock_unitsInStock = stock_unitsInStock2;
						}
					}
					stock_res.setUnitsInStock(firstNotNull_stock_unitsInStock);
					// attribute 'StockInfo.unitsOnOrder'
					Integer firstNotNull_stock_unitsOnOrder = Util.getIntegerValue(r.getAs("stock_0.unitsOnOrder"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer stock_unitsOnOrder2 = Util.getIntegerValue(r.getAs("stock_" + i + ".unitsOnOrder"));
						if (firstNotNull_stock_unitsOnOrder != null && stock_unitsOnOrder2 != null && !firstNotNull_stock_unitsOnOrder.equals(stock_unitsOnOrder2)) {
							concern_res.addLogEvent("Data consistency problem for [StockInfo - id :"+stock_res.getId()+"]: different values found for attribute 'StockInfo.unitsOnOrder': " + firstNotNull_stock_unitsOnOrder + " and " + stock_unitsOnOrder2 + "." );
							logger.warn("Data consistency problem for [StockInfo - id :"+stock_res.getId()+"]: different values found for attribute 'StockInfo.unitsOnOrder': " + firstNotNull_stock_unitsOnOrder + " and " + stock_unitsOnOrder2 + "." );
						}
						if (firstNotNull_stock_unitsOnOrder == null && stock_unitsOnOrder2 != null) {
							firstNotNull_stock_unitsOnOrder = stock_unitsOnOrder2;
						}
					}
					stock_res.setUnitsOnOrder(firstNotNull_stock_unitsOnOrder);
					// attribute 'ProductInfo.id'
					Integer firstNotNull_product_id = Util.getIntegerValue(r.getAs("product_0.id"));
					product_res.setId(firstNotNull_product_id);
					// attribute 'ProductInfo.name'
					String firstNotNull_product_name = Util.getStringValue(r.getAs("product_0.name"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String product_name2 = Util.getStringValue(r.getAs("product_" + i + ".name"));
						if (firstNotNull_product_name != null && product_name2 != null && !firstNotNull_product_name.equals(product_name2)) {
							concern_res.addLogEvent("Data consistency problem for [ProductInfo - id :"+product_res.getId()+"]: different values found for attribute 'ProductInfo.name': " + firstNotNull_product_name + " and " + product_name2 + "." );
							logger.warn("Data consistency problem for [ProductInfo - id :"+product_res.getId()+"]: different values found for attribute 'ProductInfo.name': " + firstNotNull_product_name + " and " + product_name2 + "." );
						}
						if (firstNotNull_product_name == null && product_name2 != null) {
							firstNotNull_product_name = product_name2;
						}
					}
					product_res.setName(firstNotNull_product_name);
					// attribute 'ProductInfo.supplierRef'
					Integer firstNotNull_product_supplierRef = Util.getIntegerValue(r.getAs("product_0.supplierRef"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer product_supplierRef2 = Util.getIntegerValue(r.getAs("product_" + i + ".supplierRef"));
						if (firstNotNull_product_supplierRef != null && product_supplierRef2 != null && !firstNotNull_product_supplierRef.equals(product_supplierRef2)) {
							concern_res.addLogEvent("Data consistency problem for [ProductInfo - id :"+product_res.getId()+"]: different values found for attribute 'ProductInfo.supplierRef': " + firstNotNull_product_supplierRef + " and " + product_supplierRef2 + "." );
							logger.warn("Data consistency problem for [ProductInfo - id :"+product_res.getId()+"]: different values found for attribute 'ProductInfo.supplierRef': " + firstNotNull_product_supplierRef + " and " + product_supplierRef2 + "." );
						}
						if (firstNotNull_product_supplierRef == null && product_supplierRef2 != null) {
							firstNotNull_product_supplierRef = product_supplierRef2;
						}
					}
					product_res.setSupplierRef(firstNotNull_product_supplierRef);
					// attribute 'ProductInfo.categoryRef'
					Integer firstNotNull_product_categoryRef = Util.getIntegerValue(r.getAs("product_0.categoryRef"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer product_categoryRef2 = Util.getIntegerValue(r.getAs("product_" + i + ".categoryRef"));
						if (firstNotNull_product_categoryRef != null && product_categoryRef2 != null && !firstNotNull_product_categoryRef.equals(product_categoryRef2)) {
							concern_res.addLogEvent("Data consistency problem for [ProductInfo - id :"+product_res.getId()+"]: different values found for attribute 'ProductInfo.categoryRef': " + firstNotNull_product_categoryRef + " and " + product_categoryRef2 + "." );
							logger.warn("Data consistency problem for [ProductInfo - id :"+product_res.getId()+"]: different values found for attribute 'ProductInfo.categoryRef': " + firstNotNull_product_categoryRef + " and " + product_categoryRef2 + "." );
						}
						if (firstNotNull_product_categoryRef == null && product_categoryRef2 != null) {
							firstNotNull_product_categoryRef = product_categoryRef2;
						}
					}
					product_res.setCategoryRef(firstNotNull_product_categoryRef);
					// attribute 'ProductInfo.quantityPerUnit'
					String firstNotNull_product_quantityPerUnit = Util.getStringValue(r.getAs("product_0.quantityPerUnit"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String product_quantityPerUnit2 = Util.getStringValue(r.getAs("product_" + i + ".quantityPerUnit"));
						if (firstNotNull_product_quantityPerUnit != null && product_quantityPerUnit2 != null && !firstNotNull_product_quantityPerUnit.equals(product_quantityPerUnit2)) {
							concern_res.addLogEvent("Data consistency problem for [ProductInfo - id :"+product_res.getId()+"]: different values found for attribute 'ProductInfo.quantityPerUnit': " + firstNotNull_product_quantityPerUnit + " and " + product_quantityPerUnit2 + "." );
							logger.warn("Data consistency problem for [ProductInfo - id :"+product_res.getId()+"]: different values found for attribute 'ProductInfo.quantityPerUnit': " + firstNotNull_product_quantityPerUnit + " and " + product_quantityPerUnit2 + "." );
						}
						if (firstNotNull_product_quantityPerUnit == null && product_quantityPerUnit2 != null) {
							firstNotNull_product_quantityPerUnit = product_quantityPerUnit2;
						}
					}
					product_res.setQuantityPerUnit(firstNotNull_product_quantityPerUnit);
					// attribute 'ProductInfo.unitPrice'
					Double firstNotNull_product_unitPrice = Util.getDoubleValue(r.getAs("product_0.unitPrice"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Double product_unitPrice2 = Util.getDoubleValue(r.getAs("product_" + i + ".unitPrice"));
						if (firstNotNull_product_unitPrice != null && product_unitPrice2 != null && !firstNotNull_product_unitPrice.equals(product_unitPrice2)) {
							concern_res.addLogEvent("Data consistency problem for [ProductInfo - id :"+product_res.getId()+"]: different values found for attribute 'ProductInfo.unitPrice': " + firstNotNull_product_unitPrice + " and " + product_unitPrice2 + "." );
							logger.warn("Data consistency problem for [ProductInfo - id :"+product_res.getId()+"]: different values found for attribute 'ProductInfo.unitPrice': " + firstNotNull_product_unitPrice + " and " + product_unitPrice2 + "." );
						}
						if (firstNotNull_product_unitPrice == null && product_unitPrice2 != null) {
							firstNotNull_product_unitPrice = product_unitPrice2;
						}
					}
					product_res.setUnitPrice(firstNotNull_product_unitPrice);
					// attribute 'ProductInfo.reorderLevel'
					Integer firstNotNull_product_reorderLevel = Util.getIntegerValue(r.getAs("product_0.reorderLevel"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer product_reorderLevel2 = Util.getIntegerValue(r.getAs("product_" + i + ".reorderLevel"));
						if (firstNotNull_product_reorderLevel != null && product_reorderLevel2 != null && !firstNotNull_product_reorderLevel.equals(product_reorderLevel2)) {
							concern_res.addLogEvent("Data consistency problem for [ProductInfo - id :"+product_res.getId()+"]: different values found for attribute 'ProductInfo.reorderLevel': " + firstNotNull_product_reorderLevel + " and " + product_reorderLevel2 + "." );
							logger.warn("Data consistency problem for [ProductInfo - id :"+product_res.getId()+"]: different values found for attribute 'ProductInfo.reorderLevel': " + firstNotNull_product_reorderLevel + " and " + product_reorderLevel2 + "." );
						}
						if (firstNotNull_product_reorderLevel == null && product_reorderLevel2 != null) {
							firstNotNull_product_reorderLevel = product_reorderLevel2;
						}
					}
					product_res.setReorderLevel(firstNotNull_product_reorderLevel);
					// attribute 'ProductInfo.discontinued'
					Boolean firstNotNull_product_discontinued = Util.getBooleanValue(r.getAs("product_0.discontinued"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Boolean product_discontinued2 = Util.getBooleanValue(r.getAs("product_" + i + ".discontinued"));
						if (firstNotNull_product_discontinued != null && product_discontinued2 != null && !firstNotNull_product_discontinued.equals(product_discontinued2)) {
							concern_res.addLogEvent("Data consistency problem for [ProductInfo - id :"+product_res.getId()+"]: different values found for attribute 'ProductInfo.discontinued': " + firstNotNull_product_discontinued + " and " + product_discontinued2 + "." );
							logger.warn("Data consistency problem for [ProductInfo - id :"+product_res.getId()+"]: different values found for attribute 'ProductInfo.discontinued': " + firstNotNull_product_discontinued + " and " + product_discontinued2 + "." );
						}
						if (firstNotNull_product_discontinued == null && product_discontinued2 != null) {
							firstNotNull_product_discontinued = product_discontinued2;
						}
					}
					product_res.setDiscontinued(firstNotNull_product_discontinued);
	
					concern_res.setStock(stock_res);
					concern_res.setProduct(product_res);
					return concern_res;
		}
		, Encoders.bean(Concern.class));
	
	}
	
	//Empty arguments
	public Dataset<Concern> getConcernList(){
		 return getConcernList(null,null);
	}
	
	public abstract Dataset<Concern> getConcernList(
		Condition<StockInfoAttribute> stock_condition,
		Condition<ProductInfoAttribute> product_condition);
	
	public Dataset<Concern> getConcernListByStockCondition(
		Condition<StockInfoAttribute> stock_condition
	){
		return getConcernList(stock_condition, null);
	}
	
	public Concern getConcernByStock(StockInfo stock) {
		Condition<StockInfoAttribute> cond = null;
		cond = Condition.simple(StockInfoAttribute.id, Operator.EQUALS, stock.getId());
		Dataset<Concern> res = getConcernListByStockCondition(cond);
		List<Concern> list = res.collectAsList();
		if(list.size() > 0)
			return list.get(0);
		else
			return null;
	}
	public Dataset<Concern> getConcernListByProductCondition(
		Condition<ProductInfoAttribute> product_condition
	){
		return getConcernList(null, product_condition);
	}
	
	public Concern getConcernByProduct(ProductInfo product) {
		Condition<ProductInfoAttribute> cond = null;
		cond = Condition.simple(ProductInfoAttribute.id, Operator.EQUALS, product.getId());
		Dataset<Concern> res = getConcernListByProductCondition(cond);
		List<Concern> list = res.collectAsList();
		if(list.size() > 0)
			return list.get(0);
		else
			return null;
	}
	
	public abstract void insertConcern(Concern concern);
	
	
	
	public 	abstract boolean insertConcernInRefStructStockInfoPairsInRedisDB(Concern concern);
	
	 public void insertConcern(StockInfo stock ,ProductInfo product ){
		Concern concern = new Concern();
		concern.setStock(stock);
		concern.setProduct(product);
		insertConcern(concern);
	}
	
	
	
	public abstract void deleteConcernList(
		conditions.Condition<conditions.StockInfoAttribute> stock_condition,
		conditions.Condition<conditions.ProductInfoAttribute> product_condition);
	
	public void deleteConcernListByStockCondition(
		conditions.Condition<conditions.StockInfoAttribute> stock_condition
	){
		deleteConcernList(stock_condition, null);
	}
	
	public void deleteConcernByStock(pojo.StockInfo stock) {
		// TODO using id for selecting
		return;
	}
	public void deleteConcernListByProductCondition(
		conditions.Condition<conditions.ProductInfoAttribute> product_condition
	){
		deleteConcernList(null, product_condition);
	}
	
	public void deleteConcernByProduct(pojo.ProductInfo product) {
		// TODO using id for selecting
		return;
	}
		
}
