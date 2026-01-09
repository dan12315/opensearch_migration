# OpenSearch 迁移工具

一个强大的基于Python的工具，用于使用快照和增量迁移策略将数据从Elasticsearch迁移到OpenSearch。

## 功能特性

- **增量迁移**：使用Logstash进行基于时间的增量数据同步
- **网络弹性**：内置重试机制和连接监控
- **灵活批处理**：根据数据时间间隔动态调整批大小
- **全面日志记录**：详细的进度跟踪和错误报告

## 架构

该工具由以下模块组成：

| 模块 | 描述 |
|------|------|
| `config.py` | 集中配置文件 |
| `opensearch_helper.py` | OpenSearch集群连接和数据查询 |
| `logstash_helper.py` | Logstash配置和增量同步执行 |
| `migration_workflow_helper.py` | 迁移工作流程控制和进度管理 |
| `main.py` | 主入口文件 |

## 前置条件

### 1. 系统要求
- Python 3.7+
- Java 8 或 Java 11（用于Logstash）
- 对源和目标OpenSearch集群的网络访问

### 2. 安装Logstash

- 按照官方Logstash文档进行安装：https://www.elastic.co/guide/en/logstash/7.10/installing-logstash.html
- 建议使用Logstash 7.10.2以兼容ElasticSearch和OpenSearch

## 配置

### 1. 下载脚本

1. 克隆仓库：
```bash
git clone https://github.com/dan12315/opensearch_migration.git
cd opensearch_migration
```

2. 安装Python依赖：
```bash
pip install -r requirements.txt
```

### 2. 脚本配置

编辑 `config.py` 中的配置：

```python
# Elasticsearch/OpenSearch 端点
SOURCE_ES = "http://10.0.xxx.xxx:9200"
TARGET_ES = "https://xxxxx.ap-southeast-1.es.amazonaws.com:443"

# Logstash 配置
TEMPLATE_PATH = "./logstash/es-migration-batch.conf"
JAVA_HOME = "/usr/lib/jvm/java-11-amazon-corretto"
LS_HOME = "./logstash/logstash-7.10.2"

# 迁移设置
TIMESTAMP_FIELD = "recent_view_timestamp"
SNAPSHOT_REPO = "migration_assistant_repo"
INDEX_NAME = "*"

# 文件路径
LOG_FILE = "./logs/migration.log"
PROGRESS_FILE = "./logs/.snapshot_migration_progress"

# AWS 设置
AWS_DEFAULT_REGION = "ap-southeast-1"
```

（可选）编辑 `main.py` 中的OpenSearch连接认证方法：

```python
source_helper = OpenSearchMigrationHelper(SOURCE_ES, auth=auth)
target_helper = OpenSearchMigrationHelper(TARGET_ES, aws_region=aws_region, auth=auth)
```

### 3. （可选）Logstash配置

- 更新位于 `./logstash/es-migration-batch.conf` 的Logstash配置模板
- 参考官方文档：https://www.elastic.co/guide/en/logstash/7.10/config-examples.html

## 使用方法

### 直接执行

```bash
python main.py
```

### 程序化使用

```python
from config import (
    SOURCE_ES, TARGET_ES, TIMESTAMP_FIELD, SNAPSHOT_REPO, PROGRESS_FILE, LOG_FILE
)
from opensearch_helper import OpenSearchMigrationHelper
from logstash_helper import LogStashHelper
from migration_workflow_helper import OpenSearchMigrationWorkflowHelper

# 初始化帮助类
source_helper = OpenSearchMigrationHelper(SOURCE_ES)
target_helper = OpenSearchMigrationHelper(TARGET_ES, aws_region='ap-southeast-1')
logstash_helper = LogStashHelper()
migration_workflow_helper = OpenSearchMigrationWorkflowHelper(
    source_helper=source_helper,
    target_helper=target_helper,
    logstash_helper=logstash_helper
)

# 开始迁移
migration_workflow_helper.run_migration()
```

### 迁移流程

1. **基线检测**：自动从快照或目标集群查找迁移起点
2. **用户确认**：迁移前提示确认起始时间
3. **增量同步**：使用Logstash同步数据，支持自动批处理
4. **近实时同步**：当差距 <= 5分钟时，提示停止写入以进行最终切换
5. **进度跟踪**：保存进度以恢复中断的迁移

## 文件结构

```
opensearch_migration/
├── config.py                          # 集中配置文件
├── opensearch_helper.py               # OpenSearch客户端帮助类
├── logstash_helper.py                 # Logstash执行帮助类
├── migration_workflow_helper.py       # 迁移工作流程控制
├── main.py                            # 主入口文件
├── requirements.txt                   # Python依赖
├── README.md                          # 英文文档
├── README_zh.md                       # 中文文档
├── logstash/
│   ├── es-migration-batch.conf        # Logstash模板
│   └── logstash-7.10.2/               # Logstash运行时
└── logs/
    ├── migration.log                  # 迁移日志
    └── .snapshot_migration_progress   # 进度跟踪
```

## 贡献

1. Fork 仓库
2. 创建功能分支
3. 进行更改
4. 如适用，添加测试
5. 提交拉取请求

## 许可证

本项目采用MIT许可证。

## 支持

如有问题和疑问：
- 在GitHub上创建issue
- 查看故障排除部分
- 查阅Logstash和OpenSearch文档

## 致谢

- 基于OpenSearch Python客户端
- 使用Logstash进行可靠的数据传输
- 受Elasticsearch迁移最佳实践启发
