## 1. 背景

本项目主要用于数据工程实践练习，其中主要的目的是通过`Python + Spark + Airflow + Clickhouse`的形式来对Backblaze硬盘的近几年数据做清洗&转换和统计，并将统计数据结果保存至Clickhouse中，通过Flask暴露数据查询API配合着前端ECharts进行可视化图表渲染，整体的技术架构如下所示：

![image-20240721162332019](https://zchengb-images.oss-cn-shenzhen.aliyuncs.com/image-20240721162332019.png)

其中Backblaze数据文件来源于官网（下载地址：https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data
），涵盖了近几年的数据，数据格式为CSV，整个练习的上下文如下所示：

![image-20240721162629892](https://zchengb-images.oss-cn-shenzhen.aliyuncs.com/image-20240721162629892.png)

![image-20240721162702256](https://zchengb-images.oss-cn-shenzhen.aliyuncs.com/image-20240721162702256.png)

需要做的练习包含以下2个部分：

- 每日摘要（按日期）

  - 驱动器数量

  - 驱动器故障

- 每年摘要（按年份）
  - 按品牌统计的驱动器故障数量

该练习完成后可通过前端视图来获取以上对应的数据统计信息：

![image-20240721165732838](https://zchengb-images.oss-cn-shenzhen.aliyuncs.com/image-20240721165732838.png)

## 2. 结构

整个项目大体上包含以下几个核心文件夹和文件：

- `airflow`：涵盖了DAG流水线、`Dockerfile`以及一份`docker-compose.yaml`用于快速启动Airflow环境，其中本次练习的流水线是`process_hard_drive_data_dag.py`
- `app`：涵盖了前后端的内容
  - `backend`：使用Flask搭建的后端环境，包含了后端提供的数据API
  - `frontend`：前端图表视图环境，主要语言是React，使用的图形渲染框架是ECharts
- `datalake`：数据湖，以CSV格式存储了主要的数据文件，用于挂载至`Docker`环境中以供存取相关的数据（这个部分当然可以用HDFS / S3来替代）
- `downloads`：用于存储从Backblaze官网下载下来的压缩包文件
- `extracted`：用于存储解压缩出来的CSV文件
- `download_and_organize_backblaze_data.py`：一份用于从Backblaze下载测试数据的脚本

## 3. 数据获取

 数据获取指的是从Backblaze官网将数据下载到本地，并将多份CSV整合成单独一份CSV并存储于`datalake`目录下的`all_data.csv`

以上步骤可通过在项目根目录下直接执行`python3 download_and_organize_backblaze_data.py`实现，也可以通过文件中的`quarters`和`years`来控制需要拉取的数据范围

## 4. Airflow

>**Tips**：请确保Docker的内存 > 8G，否则可能会导致DAG触发后持续位于阻塞状态

根据上下文，需要在Airflow中起一个DAG用于后续的整个数据处理 - 发布的过程，其中首要`airflow`启动可通过以下命令进行：

```shell
# 进入airflow目录
$ cd airflow

# 启动Airflow平台
$ docker-compose up -d

# 确认Airflow的运行状态
$ docker-compose ps
NAME                          COMMAND                  SERVICE             STATUS              PORTS
airflow-airflow-init-1        "/bin/bash -c 'if [[…"   airflow-init        exited (0)          
airflow-airflow-scheduler-1   "/usr/bin/dumb-init …"   airflow-scheduler   running (healthy)   8080/tcp
airflow-airflow-triggerer-1   "/usr/bin/dumb-init …"   airflow-triggerer   running (healthy)   8080/tcp
airflow-airflow-webserver-1   "/usr/bin/dumb-init …"   airflow-webserver   running (healthy)   0.0.0.0:8080->8080/tcp, :::8080->8080/tcp
airflow-airflow-worker-1      "/usr/bin/dumb-init …"   airflow-worker      running (healthy)   8080/tcp
airflow-postgres-1            "docker-entrypoint.s…"   postgres            running (healthy)   5432/tcp
airflow-redis-1               "docker-entrypoint.s…"   redis               running (healthy)   6379/tcp
```

启动完成后的默认UI地址为：http://localhost:8080/
Airflow平台默认的账号与密码均是`airflow`，其中Airflow自制镜像是因为需要使用到`Spark`和`Clickhouse-Driver`等相关Python依赖包

## 5. Spark & Clickhouse

启动完Airflow后，需要进一步启动Spark工作节点以及Clickhouse作为存储数据的DB，可通过以下命令进行：

```shell
# Tips：执行以下命令前，请确保回到了项目根目录
$ docker-compose up -d

# 确认Spark和Clickhouse的启动状态
$ docker-compose ps
NAME                COMMAND                  SERVICE             STATUS              PORTS
clickhouse-server   "/entrypoint.sh"         clickhouse-server   running             0.0.0.0:8123->8123/tcp, :::8123->8123/tcp, 0.0.0.0:9000->9000/tcp, :::9000->9000/tcp, 9009/tcp
spark-master        "/opt/bitnami/script…"   spark-master        running             0.0.0.0:7077->7077/tcp, :::7077->7077/tcp, 0.0.0.0:8888->8080/tcp, :::8888->8080/tcp
spark-worker-1      "/opt/bitnami/script…"   spark-worker-1      running 
```

其中Clickhouse暴露的端口号为9000（TCP）和8123（HTTP），Spark Master节点暴露的端口号为`7077`

## 5. 核心DAG

核心数据处理的DAG可参考`airflow - dags - process_hard_drive_data_dag.py`，这一份DAG中，会从`datalake`中加载数据，并对数据进行统计计算，随后存储到`clickhouse`中，相关步骤如下所示：

![image-20240721165058104](https://zchengb-images.oss-cn-shenzhen.aliyuncs.com/image-20240721165058104.png)

对应的DAG名称为：`process_hard_drive_data_spark`

## 6. Backend

后端的环境通过`poetry`包管理器进行管理，需要通过以下命令才可运行：

```shell
# 安装相关依赖
$ poetry install
# 进入VENV环境
$ poetry shell
# 启动服务
$ flask run -p 5000
```

对应暴露的API为以下：

- `[GET] /api/daily_summary`：用于获取按天统计数据
- `[GET] /api/yearly_summary`：用于获取按年统计数据

## 7. Frontend

```shell
# 安装依赖
$ npm install
# 快速启动
$ npm run start
```

对应的视图地址是：http://localhost:3000/
