# Timestamp Store
#### 基于文件的时序存储NoSql型数据库，适合存储历史数据
**使用方法：**
&emsp;运行**StartTSServer**启动服务，**conf.properties**中可修改数据文件保存目录
&emsp;运行**StartTSTerminal**启动终端窗口，输入**help**可打印帮助说明
&emsp;运行**ClientTest**启动测试类，通过API调用进行查询和插入。

**支持命令：**
- **create**  创建Store
`create {name:"storeName",columns:[{name:"columnName",type:columnType(int类型，具体参照Column类的常量),length:长度(该属性只支持String类型)}]}`
- **put**  插入数据
`put storeName id timestamp column1 column2 ... columnN`
- **puts**  批量插入
`只支持在API中调用`
- **get**  查找数据
`get storeName id minTimestamp maxTimestamp`
- **getcount** 查找数据（数量）
`getcount storeName id minTimestamp maxTimestamp`
- **list** 查看Store列表
`list`
- **help** 打印帮助说明
`help`


