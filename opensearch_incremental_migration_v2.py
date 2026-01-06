#!/usr/bin/env python3
import os
import sys
import json
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse
import time

# Configuration
TEMPLATE_PATH = "./logstash/es-migration-batch.conf"
JAVA_HOME = "/usr/lib/jvm/java-11-amazon-corretto"
LS_HOME = "./logstash/logstash-7.10.2"
LOG_FILE = "./logs/migration.log"
PROGRESS_FILE = "./logs/.snapshot_migration_progress"
SOURCE_ES = "http://10.0.xxx.xxx:9200"
TARGET_ES = "https://xxxxx.ap-southeast-1.es.amazonaws.com:443"
TIMESTAMP_FIELD = "recent_view_timestamp"
SNAPSHOT_REPO = "migration_assistant_repo"

class OpenSearchMigrationHelper:
    def __init__(self, es_url, aws_region=None, auth=None):
        self.es_url = es_url
        self.aws_region = aws_region
        self.auth = auth
        self.client = self._create_client()
        self.check_cluster_health()
    
    def _create_client(self):
        """Create OpenSearch client"""
        try:
            from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth
            import boto3
        except ImportError:
            raise Exception("Please install opensearch-py and boto3: pip install opensearch-py boto3")
        
        parsed_url = urlparse(self.es_url)
        host = parsed_url.hostname
        port = parsed_url.port or (443 if parsed_url.scheme == 'https' else 9200)
        use_ssl = parsed_url.scheme == 'https'
        
        # Detect if it's AWS OpenSearch
        is_aws_opensearch = 'es.amazonaws.com' in host
        
        if is_aws_opensearch:
            # AWS OpenSearch connection
            try:
                credentials = boto3.Session().get_credentials()
                auth = AWSV4SignerAuth(credentials, self.aws_region or 'us-east-1', 'es')
                
                client = OpenSearch(
                    hosts=[{'host': host, 'port': port}],
                    http_auth=auth,
                    use_ssl=True,
                    verify_certs=True,
                    connection_class=RequestsHttpConnection,
                    pool_maxsize=20
                )
                print(f"Connected to {host} using AWS IAM authentication")
                return client
            except Exception as e:
                print(f"AWS IAM authentication failed: {e}")
                if self.auth:
                    print("Trying username/password authentication...")
                else:
                    raise Exception("AWS OpenSearch connection failed, please check IAM permissions or provide username/password")
        
        # Standard OpenSearch connection or AWS fallback connection
        client = OpenSearch(
            hosts=[{'host': host, 'port': port}],
            http_auth=self.auth,
            http_compress=True,
            use_ssl=use_ssl,
            verify_certs=use_ssl,
            ssl_assert_hostname=False,
            ssl_show_warn=False
        )
        
        auth_type = "username/password" if self.auth else "no authentication"
        print(f"Connected to {host} using {auth_type}")
        return client
    
    def check_cluster_health(self):
        """Check cluster health status"""
        try:
            health = self.client.cluster.health(timeout=10)
            if health["status"] == "red":
                raise Exception(f"Cluster status abnormal: {health['status']}")
            print(f"Cluster {self.es_url} health status: {health['status']}")
            return health["status"]
        except Exception as e:
            raise Exception(f"Unable to connect to cluster {self.es_url}: {e}")
    
    def get_latest_timestamp(self, timestamp_field, index="*"):
        """Get latest document timestamp from all indices with connection check"""
        try:
            # Check cluster status first
            self.check_cluster_health()
            
            response = self.client.search(
                index=index,
                body={
                    "size": 1,
                    "sort": [{timestamp_field: {"order": "desc"}}],
                    "query": {"exists": {"field": timestamp_field}}
                }
            )
            hits = response["hits"]["hits"]
            if hits:
                return hits[0]["_source"][timestamp_field]
            return None
        except Exception as e:
            raise Exception(f"Failed to get latest timestamp: {e}")
    
    def get_snapshot_start_time(self, snapshot_repo):
        """Get latest snapshot start time"""
        try:
            response = self.client.snapshot.get(repository=snapshot_repo, snapshot="_all")
            snapshots = response["snapshots"]
            if snapshots:
                latest_snapshot = max(snapshots, key=lambda x: x["start_time_in_millis"])
                return latest_snapshot["start_time"]
            return None
        except Exception as e:
            print(f"Warning: Failed to get snapshot time: {e}")
            return None
    
    def get_earliest_timestamp(self, timestamp_field, index="*"):
        """Get earliest document timestamp from all indices"""
        try:
            # Check cluster status first
            self.check_cluster_health()

            response = self.client.search(
                index=index,
                body={
                    "size": 1,
                    "sort": [{timestamp_field: {"order": "asc"}}],
                    "query": {"exists": {"field": timestamp_field}}
                }
            )
            hits = response["hits"]["hits"]
            if hits:
                return hits[0]["_source"][timestamp_field]
            return None
        except Exception as e:
            raise Exception(f"Failed to get earliest timestamp: {e}")

class LogStashHelper:
    def __init__(self, ls_home=LS_HOME, java_home=JAVA_HOME, template_path=TEMPLATE_PATH, source_es_endpoint=SOURCE_ES, target_es_endpoint=TARGET_ES, log_file=LOG_FILE):
        self.ls_home = ls_home
        self.java_home = java_home
        self.template_path = template_path
        self.source_es_endpoint = source_es_endpoint
        self.target_es_endpoint = target_es_endpoint
        self.log_file = log_file
        self._setup_environment()
        self._check_installation()
        self.base_config = self._setup_template()
    
    def _setup_template(self):
        """Setup Logstash configuration file path"""
        try:
            base_config = f"/tmp/template_{os.getpid()}_.conf"
            # Read template and replace query
            with open(TEMPLATE_PATH, "r") as f:
                template = f.read()
            
            with open(base_config, "w") as f:
                f.write(template.replace("SOURCE_ES_ENDPOINT", self.source_es_endpoint).
                        replace("TARGET_ES_ENDPOINT", self.target_es_endpoint).
                        replace("LOG_FILE", self.log_file))
        except Exception as e:
            raise Exception(f"Failed to setup Logstash configuration template: {e}")

        return base_config

    def _setup_environment(self):
        """Setup environment variables"""
        os.environ["JAVA_HOME"] = self.java_home
        os.environ["PATH"] = f"{self.java_home}/bin:{os.environ['PATH']}"
        os.environ["LS_HOME"] = self.ls_home
    
    def _check_installation(self):
        """Check local Logstash installation"""
        try:
            # Check Logstash directory
            if not Path(self.ls_home).exists():
                raise Exception(f"Logstash directory does not exist: {self.ls_home}")
            
            # Check Logstash executable
            logstash_bin = Path(self.ls_home) / "bin" / "logstash"
            if not logstash_bin.exists():
                raise Exception(f"Logstash executable does not exist: {logstash_bin}")
            
            # Check Java
            java_bin = Path(self.java_home) / "bin" / "java"
            if not java_bin.exists():
                raise Exception(f"Java executable does not exist: {java_bin}")
            
            # Check configuration template
            if not Path(self.template_path).exists():
                raise Exception(f"Logstash configuration template does not exist: {self.template_path}")
            
            print(f"Logstash installation check passed: {self.ls_home}")
            
        except Exception as e:
            raise Exception(f"Logstash installation check failed: {e}")
    
    def run_incremental_sync(self, start_time, end_time, timestamp_field, max_retries=3):
        """Execute incremental sync with retry mechanism"""
        batch_config = None
        for attempt in range(max_retries):
            try:
                query = json.dumps({
                    "query": {
                        "range": {
                            timestamp_field: {
                                "gte": start_time,
                                "lt": end_time
                            }
                        }
                    }
                })
                
                batch_config = f"/tmp/incremental_{os.getpid()}_{attempt}.conf"
                
                # Read template and replace query
                with open(self.base_config, "r") as f:
                    template = f.read()
                
                with open(batch_config, "w") as f:
                    f.write(template.replace("BATCH_QUERY_PLACEHOLDER", query))
                
                print(f"Incremental sync {start_time} -> {end_time} (attempt {attempt + 1}/{max_retries})")
                
                result = subprocess.run(
                    [f"{self.ls_home}/bin/logstash", "-f", batch_config],
                    capture_output=True, text=True, timeout=3600
                )
                
                if result.returncode == 0:
                    print(f"Incremental sync successful (attempt {attempt + 1})")
                    return True
                else:
                    error_msg = f"Logstash execution failed (attempt {attempt + 1}): return code={result.returncode}"
                    if result.stderr:
                        error_msg += f", error output: {result.stderr[-500:]}"  # Only show last 500 characters
                    print(error_msg)
                    
                    if attempt < max_retries - 1:
                        wait_time = (attempt + 1) * 30  # Incremental wait time
                        print(f"Waiting {wait_time} seconds before retry...")
                        time.sleep(wait_time)
                    else:
                        raise Exception(f"Logstash execution failed after {max_retries} retries: {result.stderr}")
                
            except subprocess.TimeoutExpired:
                error_msg = f"Logstash execution timeout (attempt {attempt + 1})"
                print(error_msg)
                if attempt < max_retries - 1:
                    print("Retrying after timeout...")
                else:
                    raise Exception("Logstash execution timeout, maximum retries reached")
            except Exception as e:
                error_msg = f"Incremental sync exception (attempt {attempt + 1}): {e}"
                print(error_msg)
                if attempt < max_retries - 1:
                    print("Retrying after exception...")
                else:
                    raise Exception(f"Incremental sync failed after {max_retries} retries: {e}")
            finally:
                # Clean up temporary configuration file
                if batch_config and Path(batch_config).exists():
                    os.remove(batch_config)
        
        return False




class OpenSearchMigrationWorkflowHelper:
    def __init__(self, source_helper, target_helper, logstash_helper, timestamp_field=TIMESTAMP_FIELD, snapshot_repo=SNAPSHOT_REPO, progress_file=PROGRESS_FILE):
        self.source_helper = source_helper
        self.target_helper = target_helper
        self.timestamp_field = timestamp_field
        self.snapshot_repo = snapshot_repo
        self.logstash_helper = logstash_helper
        self.progress_file = progress_file

    def _log(self, msg, log_file=LOG_FILE):
        """Output to both file and console"""
        timestamp = datetime.now().strftime("%a %b %d %H:%M:%S UTC %Y")
        log_msg = f"{timestamp}: {msg}"
        print(log_msg)
        try:
            with open(log_file, "a") as f:
                f.write(log_msg + "\n")
        except Exception as e:
            print(f"Failed to write to log file: {e}")

    @staticmethod
    def _time_diff_minutes(time1, time2):
        """Calculate time difference in minutes"""
        try:
            dt1 = datetime.fromisoformat(time1.replace("Z", "+00:00").replace("+00:00", "+00:00") if "Z" in time1 or "+" in time1 else time1 + "+00:00")
            dt2 = datetime.fromisoformat(time2.replace("Z", "+00:00").replace("+00:00", "+00:00") if "Z" in time2 or "+" in time2 else time2 + "+00:00")
            return int((dt2 - dt1).total_seconds() / 60)
        except Exception as e:
            raise Exception(f"Time difference calculation failed: {e}")
    
    def get_gap_minutes(self, start_time):
        """Get the gap between two time points in minutes"""
        source_latest = self.source_helper.get_latest_timestamp(self.timestamp_field)
        gap_minutes = self._time_diff_minutes(start_time, source_latest)
        return gap_minutes, source_latest
    
    def get_next_time(self, start_time, gap_minutes, source_latest):

        """Calculate batch size based on time gap"""
        if gap_minutes > 1440:
            batch_hours = 12
        elif gap_minutes > 360:
            batch_hours = 6
        else:
            batch_hours = 1
    
        """Calculate next time point"""
        dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
        next_time = (dt + timedelta(hours=batch_hours)).isoformat()
        
        if next_time > source_latest:
            next_time = source_latest
        
        return next_time

    def get_start_time(self):
        """Get migration start time"""
        if Path(self.progress_file).exists():
            with open(self.progress_file, "r") as f:
                start_time = f.read().strip()
            self._log(f"Continuing from last progress: {start_time}")
            return start_time

        self._log("Getting incremental migration start time...")

        try:
            snapshot_time = self.source_helper.get_snapshot_start_time(self.snapshot_repo)
            if snapshot_time:
                start_time = snapshot_time
                self._log(f"Using source cluster snapshot time as start point: {start_time}")
            else:
                self._log("Source cluster snapshot time not found, trying to get target cluster latest data time...")
                start_time = self.target_helper.get_latest_timestamp(self.timestamp_field)
                if not start_time:
                    self._log("Target cluster has no data, using source cluster earliest time")
                    start_time = self.source_helper.get_earliest_timestamp(self.timestamp_field)
                    if not start_time:
                        self._log("Error: Unable to get any timestamp, migration paused")
                        sys.exit(1)
                    self._log(f"Using source cluster earliest time as start point: {start_time}")
                else:
                    self._log(f"Using target cluster latest time as start point: {start_time}")
        except Exception as e:
            self._log(f"Failed to get start time: {e}")
            sys.exit(1)
        
        return start_time

    def confirm_start_time(self, start_time):
        """User confirmation of start time"""
        print("=" * 60)
        self._log(f"Incremental migration start time: {start_time}")
        print("=" * 60)
        confirm = input("Confirm using this time as incremental migration start point? (y/N): ")
        if confirm.lower() not in ['y', 'yes']:
            print("Migration cancelled by user")
            sys.exit(0)
        self.save_progress(start_time)

    def handle_near_realtime(self, start_time):
        """Handle near real-time situation"""
        print("=" * 50)
        gap_minutes, source_latest = self.get_gap_minutes(start_time)
        self._log(f"Incremental migration is near real-time! Current gap: {gap_minutes} minutes")
        print("Please perform the following steps:")
        print("1. Stop business write operations")
        print("2. Enter 'y' to execute final sync...confirm sync completion")
        print("3. Switch application endpoint to target cluster, resume writes")
        print("=" * 50)

        # Wait for user confirmation to stop writes
        # Wait for user to input 'y', otherwise keep waiting
        stop_confirm = input("Confirm write operations have been stopped? (y/N): ")
        while stop_confirm.lower() not in ['y', 'yes']:
            stop_confirm = input("Did not receive 'y' input, please confirm if write operations are stopped (y/N): ")

        # Check time difference again
        gap_minutes, source_latest = self.get_gap_minutes(start_time)
        if gap_minutes > 6:
            self._log("Writes not stopped in time, current data difference is still large, continuing incremental sync")
            return False
        else:
            # Final sync
            self._log("Executing final incremental sync...")
            final_latest = datetime.now().isoformat()
            
            if self.logstash_helper.run_incremental_sync(start_time, final_latest, self.timestamp_field):
                self._log("Incremental migration completed!")
                Path(self.progress_file).unlink(missing_ok=True)
                sys.exit(0)
            else:
                self._log("Final sync failed!")
                sys.exit(1)

    def save_progress(self, start_time):
        """Save progress"""
        with open(self.progress_file, "w") as f:
            f.write(start_time)
        self._log(f"Progress saved: {start_time}")

    def run_migration(self):
        """Execute complete migration workflow"""
        self._log("Starting post-snapshot incremental migration...")
        
        # Get start time
        start_time = self.get_start_time()
        self.confirm_start_time(start_time)
        
        # Loop tracking until near real-time
        while True:
            try:
                gap_minutes, source_latest = self.get_gap_minutes(start_time)
                
                self._log(f"Current progress: {start_time}, source cluster latest: {source_latest}, gap: {gap_minutes} minutes")
                
                if gap_minutes <= 5:
                    if self.handle_near_realtime(start_time):
                        break
                
                # Calculate batch size
                next_time = self.get_next_time(start_time, gap_minutes, source_latest)
                
                # Execute incremental sync
                if self.logstash_helper.run_incremental_sync(start_time, next_time, self.timestamp_field):
                    start_time = next_time
                    self.save_progress(start_time)
                    time.sleep(5)
                else:
                    self._log("Incremental sync failed")
                    sys.exit(1)
                    
            except Exception as e:
                self._log(f"Error occurred during migration: {e}")
                sys.exit(1)


def main():
    try:
        # Initialize
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