# 基于Spark和Hadoop的网络舆情分析及可视化平台

## 项目简介
本项目旨在构建一个基于Hadoop和Spark的大数据分析平台，实现数据的预处理、可视化探索性分析以及机器学习模型的应用。通过PyCharm连接远程虚拟机，利用SSH和SFTP协议进行集群部署和代码开发。该平台能够处理大规模数据，并提供高效的内存计算能力，为后续的数据可视化和预测分析奠定基础。

## 项目亮点

分布式集群部署：在虚拟机中部署Hadoop和Spark集群，为大数据处理提供稳定可靠的基础设施。
内存计算优化：利用Spark集群进行数据预处理和分析，充分发挥内存计算的优势，提升数据处理效率。
模块化设计：项目划分为清晰的模块，包括数据预处理、SQL探索分析和MLlib机器学习，结构清晰，易于维护和扩展。
数据持久化：将处理后的数据存储到HDFS中，确保数据的安全性和可复用性。
可视化集成：为后续使用Flask和ECharts进行数据可视化提供数据支持，实现数据的直观展现。
机器学习应用：集成朴素贝叶斯进行情感倾向判断，以及线性回归进行舆情趋势预测，提供数据驱动的决策支持。
## 技术栈
开发环境：PyCharm
远程连接：SSH, SFTP
## 大数据框架：
Hadoop (分布式文件系统 HDFS, Yet Another Resource Negotiator YARN)
Apache Spark (Spark Core, Spark SQL, Spark MLlib)
## 编程语言：Python
未来可视化：Flask, ECharts (数据已准备好，待集成)


# 模块介绍

## 1. data_analysis 模块 (数据预处理)
功能：负责对原始数据进行清洗、转换、规范化等预处理操作。
特点：
直接调用Spark集群进行内存计算，实现高效的数据处理。
为后续的探索性分析和机器学习任务提供高质量的数据输入。

## 2. data_spark_SQL 模块 (数据可视化探索性分析)
功能：对data_analysis模块处理后的数据进行探索性分析。
特点：
利用Spark SQL进行数据查询和聚合，方便进行统计分析。
将分析后的数据存储到HDFS中，方便后续使用Flask和ECharts进行可视化展示。
为数据可视化提供结构化的数据源。

## 3. data_spark_MLlib 模块 (机器学习)

功能：应用机器学习算法对数据进行预测和分类。
特点：
朴素贝叶斯 (Naive Bayes)：用于情感倾向判断，对文本数据进行情绪分类（例如：积极、消极、中性）。
线性回归 (Linear Regression)：用于舆情趋势预测，根据历史数据预测未来舆情的发展趋势。
集群部署与开发环境
本项目在虚拟机中部署了Hadoop和Spark集群。PyCharm通过SSH协议连接到虚拟机，实现远程代码编辑、调试和运行。SFTP协议用于文件传输，方便代码和数据的上传下载。

## 如何运行
环境准备：
确保你的虚拟机已安装并配置好Hadoop和Spark集群。
在本地PyCharm中配置好SSH和SFTP连接，指向你的虚拟机。

## 克隆仓库：
Bash

git clone https://github.com/YourUsername/YourRepositoryName.git
cd YourRepositoryName

上传代码：
通过PyCharm的SFTP协议将项目代码上传到虚拟机的指定目录。
运行模块：
在虚拟机中，通过Spark-submit或其他适当的方式运行各个Python模块。
例如：
Bash

spark-submit data_analysis.py
spark-submit data_spark_SQL.py
spark-submit data_spark_MLlib.py
请根据实际情况调整脚本名称和运行参数。
