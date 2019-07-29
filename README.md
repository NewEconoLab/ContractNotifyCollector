# ContractNotifyCollector
[简体中文](#zh) |    [English](#en) 

<a name="zh">简体中文</a>
## 概述 :
- 本项目的功能是获取节点合约通知数据，分析并入库，对外提供合约数据服务。

## 部署演示 :

安装git（如果已经安装则跳过） :
```
yum install git -y
```

安装 dotnet sdk :
```
rpm -Uvh https://packages.microsoft.com/config/rhel/7/packages-microsoft-prod.rpm
yum update
yum install libunwind libicu -y
yum install dotnet-sdk-2.1.200 -y
```

通过git将本工程下载到服务器 :
```
git clone https://github.com/NewEconoLab/ContractNotifyCollector.git
```

修改配置文件放在执行文件下，配置文件大致如下 :
```json
{
  "startNetType": "testnet",
  "DBConnInfoList": [
    {
      "netType": "网络类型",
      "blockConnStr": "基础数据库连接地址",
      "blockDatabase": "基础数据库名称",
      "analyConnStr": "分析数据库连接地址",
      "analyDatabase": "分析数据库名称",
      "notifyConnStr": "合约通知数据库连接地址",
      "notifyDatabase": "合约通知数据库名称",
      "NELApiUrl": "基础api请求地址"
    }
  ],
  "TaskList":  [
    {
      "taskNet": "任务网络类型",
      "taskName": "任务名称",
      "taskInfo": {
        "key1": "value1",
        "key2": "value2"
      }
    }
  ]
}
```
```json
{
  "taskList": [
    {
      "netType": "网络类型",
      "contractHash": "合约哈希",
      "notifyDisplayName": "通知名称",
      "notifyStructure": [
        {
          "name": "displayName",
          "type": "通知类型",
          "escape": "转化类型",
          "where": "通知名称"
        },
        {
          "name": "字段名称",
          "type": "通知类型",
          "escape": "转化类型"
        },
        {
          "name": "字段名称",
          "type": "通知类型",
          "escape": "转化类型",
          "decimals": "精度"
        }
      ],
      "memo": ""
    },
```


编译并运行
```
dotnet publish
cd  NeoBlock-Mongo-Storage/NeoBlockMongoStorage/NeoBlockMongoStorage/bin/Debug/netcoreapp2.0
dotnet NeoBlock-Mongo-Storage.dll
```

### 依赖工程
- [neo-cli-nel](https://github.com/NewEconoLab/neo-cli-nel)


<a name="en">English</a>
## Overview :
- The function of this project is to obtain the node contract notification data, analyze and merge it into the library, and provide contract data service to the outside.

## Deployment

install git（Skip if already installed） :
```
yum install git -y
```

install dotnet sdk :
```
rpm -Uvh https://packages.microsoft.com/config/rhel/7/packages-microsoft-prod.rpm
yum update
yum install libunwind libicu -y
yum install dotnet-sdk-2.1.200 -y
```

clone to the server :
```
git clone https://github.com/NewEconoLab/ContractNotifyCollector.git
```

Modify the configuration file under the execution file, the configuration file is roughly as follows:
```json
```json
{
  "startNetType": "testnet",
  "DBConnInfoList": [
    {
      "netType": "network type",
      "blockConnStr":"basic database connectString address",
      "blockDatabase":"basic database name",
      "analyConnStr":"analysis database connectString address",
      "analyDatabase":"analysis database name",
      "notifyConnStr":"contract notify database connectString address",
      "notifyDatabase":"contract notify database name",
      "NELApiUrl":"basic api request url"
    }
  ],
  "TaskList": [
    {
      "taskNet":"task network",
      "taskName":"task name",
      "taskInfo":{
        "key1":"value1",
        "key2":"value2"
      }
    }
  ]
}
```
```json
{
  "taskList": [
    {
      "netType": "network type",
      "contractHash": "contract hash",
      "notifyDisplayName": "notify name",
      "notifyStructure": [
        {
          "name": "displayName",
          "type": "notify data type",
          "escape": "transfer data type",
          "where": "notify name"
        },
        {
          "name": "field name",
          "type": "field type",
          "escape": "transfer field type"
        },
        {
          "name": "field name",
          "type": "field type",
          "escape": "transfer field type"
          "decimals": "precision"
        }
      ],
      "memo": ""
    },
```

Compile and run :
```
dotnet publish
cd  NeoBlock-Mongo-Storage/NeoBlockMongoStorage/NeoBlockMongoStorage/bin/Debug/netcoreapp2.0
dotnet NeoBlock-Mongo-Storage.dll
```

### dependency project
- [neo-cli-nel](https://github.com/NewEconoLab/neo-cli-nel)
