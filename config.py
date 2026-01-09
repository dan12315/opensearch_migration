#!/usr/bin/env python3
"""
Centralized configuration for OpenSearch migration tool.
All modules import settings from this file.
"""

# Elasticsearch/OpenSearch Endpoints
SOURCE_ES = "http://10.0.x.x:9200"
TARGET_ES = "https://xxxx.ap-southeast-1.es.amazonaws.com:443"

# Logstash Configuration
TEMPLATE_PATH = "./logstash/es-migration-batch.conf"
JAVA_HOME = "/usr/lib/jvm/java-11-amazon-corretto"
LS_HOME = "./logstash/logstash-7.10.2"

# Migration Settings
TIMESTAMP_FIELD = "recent_view_timestamp"
SNAPSHOT_REPO = "migration_assistant_repo"

# File Paths
LOG_FILE = "./logs/migration.log"
PROGRESS_FILE = "./logs/.snapshot_migration_progress"

# AWS Settings
AWS_DEFAULT_REGION = "ap-southeast-1"
