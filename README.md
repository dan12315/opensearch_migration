# OpenSearch Migration Tool

A robust Python-based tool for migrating data from Elasticsearch to OpenSearch using snapshot and incremental migration strategies.

## Features

- **Incremental Migration**: Time-based incremental data synchronization using Logstash
- **Network Resilience**: Built-in retry mechanisms and connection monitoring
- **Flexible Batching**: Dynamic batch size adjustment based on data time gaps
- **Comprehensive Logging**: Detailed progress tracking and error reporting

## Architecture

The tool consists of two main components:

1. **OpenSearchMigrationHelper**: Handles snapshot operations and cluster management
2. **LogStashHelper**: Manages Logstash-based incremental data transfer

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

Edit the configuration in `opensearch_incremental_migration_v2.py`:

```python
TEMPLATE_PATH = "./logstash/es-migration-batch.conf"
JAVA_HOME = "/usr/lib/jvm/java-11-amazon-corretto"
LS_HOME = "./logstash/logstash-7.10.2"
LOG_FILE = "./logs/migration.log"
PROGRESS_FILE = "./logs/.snapshot_migration_progress"
SOURCE_ES = "http://10.0.xxx.xxx:9200"
TARGET_ES = "https://xxxxx.ap-southeast-1.es.amazonaws.com:443"
TIMESTAMP_FIELD = "recent_view_timestamp"
SNAPSHOT_REPO = "migration_assistant_repo"
```

(Optional) Edit OpenSearch connection auth method in `opensearch_incremental_migration_v2.py`:

```python
source_helper = OpenSearchMigrationHelper(SOURCE_ES, auth=auth)
target_helper = OpenSearchMigrationHelper(TARGET_ES, aws_region=aws_region, auth=auth)
```

### 3. (Optional) Logstash Configuration

- Update Logstash configuration template at `./logstash/es-migration-batch.conf`:
- Refer to official document : https://www.elastic.co/guide/en/logstash/7.10/config-examples.html 

## Usage

### Direct Execution

```bash
python opensearch_incremental_migration_v2.py
```

### Programmatic Usage

```python
from opensearch_incremental_migration_v2 import (
    OpenSearchMigrationHelper,
    LogStashHelper,
    OpenSearchMigrationWorkflowHelper
)

# Initialize helpers
aws_region = os.environ.get('AWS_DEFAULT_REGION', 'ap-southeast-1')
source_helper = OpenSearchMigrationHelper(SOURCE_ES)
target_helper = OpenSearchMigrationHelper(TARGET_ES, aws_region=aws_region)
logstash_helper = LogStashHelper()
migration_workflow_helper = OpenSearchMigrationWorkflowHelper(
    source_helper=source_helper,
    target_helper=target_helper,
    logstash_helper=logstash_helper,
    snapshot_repo=SNAPSHOT_REPO,
    timestamp_field=TIMESTAMP_FIELD,
    progress_file=PROGRESS_FILE
)

# start migrate
migration_workflow_helper.run_migration()
```

### Migration Process

1. **Baseline Detection**: Automatically finds migration starting point from snapshots or target cluster
2. **User Confirmation**: Prompts to stop writes to source cluster
3. **Incremental Sync**: Syncs remaining data using Logstash with automatic batching
4. **Progress Tracking**: Saves progress to resume interrupted migrations


## File Structure

```
opensearch_migration/
├── opensearch_incremental_migration_v2.py        # Main migration script
├── README.md                                     # This file
├── logstash/
│   └── es-migration-batch.conf                   # Logstash template
│   └── logstash-7.10.2/                          # Logstash runtime
└── logs/
    ├── migration.log                             # Migration logs
    └── .snapshot_migration_progress              # Progress tracking
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
