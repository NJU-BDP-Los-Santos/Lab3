# README

```wiki
NJU BigDataProcessing Lab3.
Using MapReduce to process Big Data.
--------------------------------------------
Author/版权所有©:
HELLORPG
vigorweijia
171860596
--------------------------------------------
2020.4.27
```



## File Tree

```bash
/
README.md
.gitignore
Reference.md	# 用于记录参考
```



## 配置方法

详见：[配置方法MD](./IDEA_Config.md) [配置方法PDF](./IDEA_Config.pdf)


## 执行方式

### ReduceSideJoin
执行Jar包
```bash
hadoop jar /home/2020st27/Lab3/ReduceSide.jar /data/exercise_3 /user/2020st27/Lab3/ReduceSide
```
建立SQL
```SQL
create table 2020st27_Lab3_ReduceSideJoin(id int,order_date string,pid string,name string,price int,num int)
row format delimited fields terminated by ' ' location '/user/2020st27/Lab3/ReduceSide/';
```