databases {
	mysql relData {
		dbname : "reldata"
		host : "localhost"
		login : "root"
		password : "password"
		port : 3399
	}
	mongodb myMongoDB {
		dbname : "myMongoDB"
		host : "localhost"
		port : 27777
	}
	redis redisDB {
		host : "localhost"
		port : 6666
	}
}
//Conceptual schema
conceptual schema conceptualSchema {
	//entities
	entity type Shipper {
		id : int,
		companyName : string, 
		phone : string
		identifier {
			id
		}
	}
	entity type Product {
		id : int, 
		name : string, 
		supplierRef : int,
		categoryRef : int,
		quantityPerUnit : string,
		unitPrice : float,
		reorderLevel : int,
		discontinued : bool,
		unitsInStock : int,
		unitsOnOrder : int
		identifier {
			id
		}
	}
	entity type Supplier {
		id : int,
		address : string,
		city : string,
		companyName : string,
		contactName : string, 
		contactTitle : string,
		country : string,
		fax : string,
		homePage : string,
		phone : string,
		postalCode : string,
		region : string
		identifier {
			id
		}
	}
	entity type Customer {
		id : string,
		city : string,
		companyName : string, 
		contactName : string, 
		contactTitle : string,
		country : string, 
		fax : string, 
		phone : string, 
		postalCode : string,
		region : string, 
		address : string
		identifier {
			id,
			contactName
		}
	}
	entity type Order {
		id : int,
		freight : float,
		orderDate : date,
		requiredDate : date,
		shipAddress : string,
		shipCity : string,
		shipCountry : string,
		shipName : string,
		shipPostalCode : string, 
		shipRegion : string,
		shippedDate : date
		identifier {
			id
		}
	}
	entity type Category {
		id : int,
		categoryName : string,
		description : string,
		picture : string
		identifier {
			id
		}
	}
	entity type Employee {
		id : int,
		address : string, 
		birthDate : date,
		city : string,
		country : string,
		extension : string,
		firstname : string,
		hireDate : date,
		homePhone : string,
		lastname : string,
		photo : string,
		postalCode : string,
		region : string,
		salary : float,
		title : string,
		notes : string,
		photoPath : string,
		titleOfCourtesy : string
		identifier {
			id
		}
	}
	entity type Region {
		id : int,
		description : string
		identifier {
			id
		}
	}
	entity type Territory {
		id : int,
		description : string
		identifier {
			id
		}
	}
	//relationships
	relationship type make_by {
		order[1]: Order,
		client[0-N]: Customer
	}
	relationship type contains {
		territory[1]: Territory,
		region[0-N]: Region
	}
	relationship type are_in {
		employee[0-N]: Employee,
		territory[0-N]: Territory 
	}
	relationship type report_to {
		lowerEmployee[0-1]: Employee,
		higherEmployee[0-N]: Employee
	}
	relationship type ship_via {
		shipper[0-N]: Shipper,
		order[1]: Order
	}
	relationship type handle {
		employee[0-N]: Employee,
		order[1]: Order
	}
	relationship type insert {
		supplier[0-N]: Supplier,
		product[1]: Product
		
	}
	relationship type composed_of {
		order[0-N]: Order,
		product[0-N]: Product,
		unitPrice: float,
		quantity: int,
		discount: float
	}
	relationship type belongs_to {
		product[1]: Product,
		category[0-N]: Category
	}
}
//Physical schema
physical schemas {
	key value schema kv : redisDB {
		kvpairs categoryPairs {
			key : "CATEGORY:"[categoryid],
			value : hash {
				CategoryName,
				Description,
				Picture
			}
		}
		kvpairs stockInfoPairs {
			key : "PRODUCT:"[productid]":STOCKINFO",
			value : hash {
				UnitsInStock,
				UnitsOnOrder
			}
		}
	}
	relational schema relSchema : relData {
		table Shippers {
			columns {
				ShipperID,
				CompanyName,
				Phone
			}
		}
		table ProductsInfo {
			columns {
				ProductID,
				ProductName,
				SupplierRef,
				CategoryRef,
				QuantityPerUnit,
				UnitPrice,
				ReorderLevel,
				Discontinued
			}
			references {
				categoryR: CategoryRef -> kv.categoryPairs.categoryid
				supplierR: SupplierRef -> mongoSchema.Suppliers.SupplierID
			}
		}
		table Order_Details {
			columns {
				OrderRef,
				ProductRef,
				UnitPrice,
				Quantity,
				Discount
			}
			references {
				orderR: OrderRef -> mongoSchema.Orders.OrderID
				productR: ProductRef -> ProductsInfo.ProductID
			}
		}
	
	}
	document schema mongoSchema : myMongoDB {
		collection Customers {
			fields {
				City,
				CompanyName,
				ContactName,
				ContactTitle,
				Country,
				Fax,
				ID,
				Phone,
				PostalCode,
				Region,
				Address
			}
		}
		collection Employees {
			fields {
				Address,
				BirthDate,
				City,
				Country,
				EmployeeID,
				Extension,
				FirstName,
				HireDate,
				HomePhone,
				LastName,
				Photo,
				PostalCode,
				Region,
				Salary,
				ReportsTo,
				Title,
				Notes,
				PhotoPath,
				TitleOfCourtesy,
				territories[0-N] {
					TerritoryDescription,
					TerritoryID,
					region[1] {
						RegionDescription,
						RegionID
					}
				}
			}
			references {
				reportToRef: ReportsTo -> mongoSchema.Employees.EmployeeID
			}
		}
		collection Orders {
			fields {
				EmployeeRef,
				Freight,
				OrderDate,
				RequiredDate,
				ShipAddress,
				OrderID,
				ShipCity,
				ShipCountry,
				ShipName,
				ShipPostalCode,
				ShipRegion,
				ShipVia,
				ShippedDate,
				customer[1]{
					CustomerID,
					ContactName
				}
			}
			references {
				customerRef: customer.CustomerID -> mongoSchema.Customers.ID
				shipperRef: ShipVia -> relSchema.Shippers.ShipperID
				employeeRef: EmployeeRef -> mongoSchema.Employees.EmployeeID
			}
		}
		collection Suppliers {
			fields {
				Address,
				City, 
				CompanyName,
				contactName,
				ContactTitle,
				Country,
				Fax,
				HomePage,
				Phone, 
				PostalCode,
				Region,
				SupplierID
			}
		}
	}
}
mapping rules {
	//Employee
	conceptualSchema.Employee(id, address, birthDate, city, country, extension, firstname, hireDate, homePhone, lastname, photo, postalCode, region, salary, title, notes, photoPath, titleOfCourtesy) 
	-> mongoSchema.Employees(EmployeeID, Address, BirthDate, City, Country, Extension, FirstName, HireDate, HomePhone, LastName, Photo, PostalCode, Region, Salary, Title, Notes, PhotoPath, TitleOfCourtesy),
	
	//Region
	conceptualSchema.Region(id, description) -> mongoSchema.Employees.territories.region(RegionID, RegionDescription),
	
	//Territory
	conceptualSchema.Territory(id, description) -> mongoSchema.Employees.territories(TerritoryID, TerritoryDescription),
	
	//Product
	conceptualSchema.Product(id, name, supplierRef, categoryRef,quantityPerUnit, unitPrice, reorderLevel, discontinued) -> relSchema.ProductsInfo(ProductID, ProductName, SupplierRef, CategoryRef, QuantityPerUnit, UnitPrice, ReorderLevel, Discontinued),
	conceptualSchema.Product(id, unitsInStock, unitsOnOrder) -> kv.stockInfoPairs(productid, UnitsInStock, UnitsOnOrder),
	
	//Category
	conceptualSchema.Category(id, categoryName, description, picture) -> kv.categoryPairs(categoryid, CategoryName, Description, Picture),	
	
	//Shipper
	conceptualSchema.Shipper(id, companyName, phone) -> relSchema.Shippers(ShipperID, CompanyName, Phone),
	
	//Customer
	conceptualSchema.Customer(id, city, companyName, contactName, contactTitle, country, fax, phone, postalCode, region, address) -> mongoSchema.Customers(ID, City, CompanyName, ContactName, ContactTitle, Country, Fax, Phone, PostalCode, Region, Address),
	//Order
	conceptualSchema.Order(id, freight, orderDate, requiredDate, shipAddress, shipCity, shipCountry, shipName, shipPostalCode, shipRegion, shippedDate) 
	-> mongoSchema.Orders(OrderID, Freight, OrderDate, RequiredDate, ShipAddress, ShipCity, ShipCountry, ShipName, ShipPostalCode, ShipRegion, ShippedDate),
	
	//Supplier
	conceptualSchema.Supplier(id, address, city, companyName, contactName, contactTitle, country, fax, homePage, phone, postalCode, region) 
	-> mongoSchema.Suppliers(SupplierID, Address, City, CompanyName, contactName, ContactTitle, Country, Fax, HomePage, Phone, PostalCode, Region),
	
	//OrderDetails map
	conceptualSchema.composed_of.order -> relSchema.Order_Details.orderR,
	conceptualSchema.composed_of.product -> relSchema.Order_Details.productR,
	rel: conceptualSchema.composed_of(unitPrice, quantity, discount) -> relSchema.Order_Details(UnitPrice, Quantity, Discount),
	
	
	//Link Employee and territory
	conceptualSchema.are_in.employee -> mongoSchema.Employees.territories(),
	
	//Link Territory and Region
	conceptualSchema.contains.territory -> mongoSchema.Employees.territories.region(),
	
	//Link higher and lower employee
	conceptualSchema.report_to.lowerEmployee -> mongoSchema.Employees.reportToRef,
	
	//Link order and customer
	conceptualSchema.make_by.order -> mongoSchema.Orders.customerRef,
	
	//link category and Product
	conceptualSchema.belongs_to.product -> relSchema.ProductsInfo.categoryR,
	
	//Link supplier and product
	conceptualSchema.insert.product -> relSchema.ProductsInfo.supplierR,
	
	//Link shipper and Order
	conceptualSchema.ship_via.order -> mongoSchema.Orders.shipperRef,
	
	//link Order and Employee
	conceptualSchema.handle.order -> mongoSchema.Orders.employeeRef
	
	
}