#!/usr/bin/env python3
"""
OpenSearch Incremental Migration Tool - Main Entry

Purpose:
- Initialize connections to source and target clusters
- Configure Logstash sync tool
- Start incremental migration workflow
"""

import os
import sys

from config import (
    SOURCE_ES, TARGET_ES, TIMESTAMP_FIELD, SNAPSHOT_REPO,
    PROGRESS_FILE, LOG_FILE, AWS_DEFAULT_REGION
)
from opensearch_helper import OpenSearchMigrationHelper
from logstash_helper import LogStashHelper
from migration_workflow_helper import OpenSearchMigrationWorkflowHelper


def main():
    try:
        # Initialize
        aws_region = os.environ.get('AWS_DEFAULT_REGION', AWS_DEFAULT_REGION)
        source_helper = OpenSearchMigrationHelper(SOURCE_ES)
        target_helper = OpenSearchMigrationHelper(TARGET_ES, aws_region=aws_region)
        logstash_helper = LogStashHelper()
        migration_workflow_helper = OpenSearchMigrationWorkflowHelper(
            source_helper=source_helper,
            target_helper=target_helper,
            logstash_helper=logstash_helper,
            snapshot_repo=SNAPSHOT_REPO,
            timestamp_field=TIMESTAMP_FIELD,
            progress_file=PROGRESS_FILE,
            log_file=LOG_FILE
        )
        
        # Execute migration
        migration_workflow_helper.run_migration()
                
    except KeyboardInterrupt:
        print("\nMigration interrupted")
        sys.exit(1)
    except Exception as e:
        print(f"Migration initialization failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
