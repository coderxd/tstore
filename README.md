# tstore
Timestamp Store

适用范围
  适合存储历史记录的数据,查询是根据ID范围查找这种查询

Terminal Command(暂时支持如下)：
  create：创建Store create jsonstring({name:"storeName",columns:[{name:"columnName",type:columnType(int类型具体参照Column类的常量),length:长度(只支持String类型)}]})
 
  put：插入数据 put storeName id timestamp column1 column2 ... columnN
  
  get：查找数据 get storeName id minTimestamp maxTimestamp
  
  getcount：查找数据（数量）getcount storeName id minTimestamp maxTimestamp
