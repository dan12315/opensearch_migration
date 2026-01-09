# OpenSearch Migration Tool

A robust Python-based tool for migrating data from Elasticsearch to OpenSearch using snapshot and incremental migration strategies.

## Features

- **Incremental Migration**: Time-based incremental data synchronization using Logstash
- **Network Resilience**: Built-in retry mechanisms and connection monitoring
- **Flexible Batching**: Dynamic batch size adjustment based on data time gaps
- **Comprehensive Logging**: Detailed progress tracking and error reporting

## Architecture

The tool consists of the following modules:

| Module | Description |
|--------|-------------|
| `config.py` | Centralized configuration for all settings |
| `opensearch_helper.py` | OpenSearch cluster connection and data query |
| `logstash_helper.py` | Logstash configuration and incremental sync execution |
| `migration_workflow_helper.py` | Migration workflow control and progress management |
| `main.py` | Main entry point |

## Prerequisites

### 1. System Requirements
- Python 3.7+
- Java 8 or Java 11 (for Logstash)
- Network access to both source and target OpenSearch clusters

### 2. Install Logstash

- Follow the official Logstash documentation: https://www.elastic.co/guide/en/logstash/7.10/installing-logstash.html
- Suggest using Logstash 7.10.2 to be compatible with both ElasticSearch and OpenSearch

## Configuration

### 1. Download Script

1. Clone the repository:
```bash
git clone https://github.com/dan12315/opensearch_migration.git
cd opensearch_migration
```

2. Install Python dependencies:
```bash
pip install -r requirements.txt
```

### 2. Script Configuration

Edit the configuration in `config.py`:

```python
# Elasticsearch/OpenSearch Endpoints
SOURCE_ES = "http://10.0.xxx.xxx:9200"
TARGET_ES = "https://xxxxx.ap-southeast-1.es.amazonaws.com:443"

# Logstash Configuration
TEMPLATE_PATH = "./logstash/es-migration-batch.conf"
JAVA_HOME = "/usr/lib/jvm/java-11-amazon-corretto"
LS_HOME = "./logstash/logstash-7.10.2"

# Migration Settings
TIMESTAMP_FIELD = "recent_view_timestamp"
SNAPSHOT_REPO = "migration_assistant_repo"
INDEX_NAME = "*"

# File Paths
LOG_FILE = "./logs/migration.log"
PROGRESS_FILE = "./logs/.snapshot_migration_progress"

# AWS Settings
AWS_DEFAULT_REGION = "ap-southeast-1"
```

(Optional) Edit OpenSearch connection auth method in `main.py`:

```python
source_helper = OpenSearchMigrationHelper(SOURCE_ES, auth=auth)
target_helper = OpenSearchMigrationHelper(TARGET_ES, aws_region=aws_region, auth=auth)
```

### 3. (Optional) Logstash Configuration

- Update Logstash configuration template at `./logstash/es-migration-batch.conf`
- Refer to official document: https://www.elastic.co/guide/en/logstash/7.10/config-examples.html 

## Usage

### Direct Execution

```bash
python main.py
```

### Programmatic Usage

```python
from config import (
    SOURCE_ES, TARGET_ES, TIMESTAMP_FIELD, SNAPSHOT_REPO, PROGRESS_FILE, LOG_FILE
)
from opensearch_helper import OpenSearchMigrationHelper
from logstash_helper import LogStashHelper
from migration_workflow_helper import OpenSearchMigrationWorkflowHelper

# Initialize helpers
source_helper = OpenSearchMigrationHelper(SOURCE_ES)
target_helper = OpenSearchMigrationHelper(TARGET_ES, aws_region='ap-southeast-1')
logstash_helper = LogStashHelper()
migration_workflow_helper = OpenSearchMigrationWorkflowHelper(
    source_helper=source_helper,
    target_helper=target_helper,
    logstash_helper=logstash_helper
)

# Start migration
migration_workflow_helper.run_migration()
```

### Migration Process

1. **Baseline Detection**: Automatically finds migration starting point from snapshots or target cluster
2. **User Confirmation**: Prompts to confirm start time before migration
3. **Incremental Sync**: Syncs data using Logstash with automatic batching
4. **Near Real-time**: When gap <= 5 min, prompts to stop writes for final cutover
5. **Progress Tracking**: Saves progress to resume interrupted migrations

## File Structure

```
opensearch_migration/
├── config.py                          # Centralized configuration
├── opensearch_helper.py               # OpenSearch client helper
├── logstash_helper.py                 # Logstash execution helper
├── migration_workflow_helper.py       # Migration workflow control
├── main.py                            # Main entry point
├── requirements.txt                   # Python dependencies
├── README.md                          # English documentation
├── README_zh.md                       # Chinese documentation
├── logstash/
│   ├── es-migration-batch.conf        # Logstash template
│   └── logstash-7.10.2/               # Logstash runtime
└── logs/
    ├── migration.log                  # Migration logs
    └── .snapshot_migration_progress   # Progress tracking
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License.

## Support

For issues and questions:
- Create an issue on GitHub
- Check the troubleshooting section
- Review Logstash and OpenSearch documentation

## Acknowledgments

- Built on OpenSearch Python client
- Uses Logstash for reliable data transfer
- Inspired by Elasticsearch migration best practices
