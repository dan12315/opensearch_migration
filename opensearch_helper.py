#!/usr/bin/env python3
"""
OpenSearchMigrationHelper - OpenSearch cluster connection and data query helper class

Purpose:
- Create and manage OpenSearch client connections (supports AWS IAM auth and username/password auth)
- Check cluster health status
- Query latest/earliest timestamps from indices
- Retrieve snapshot information
"""

from urllib.parse import urlparse


class OpenSearchMigrationHelper:
    def __init__(self, es_url, aws_region=None, auth=None):
        self.es_url = es_url
        self.aws_region = aws_region
        self.auth = auth
        self.client = self._create_client()
        self.check_cluster_health()
    
    def _create_client(self):
        """Create OpenSearch client with auto-detection of AWS/standard connection."""
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
        """Check cluster health. Returns status string. Raises if red or unreachable."""
        try:
            health = self.client.cluster.health(timeout=10)
            if health["status"] == "red":
                raise Exception(f"Cluster status abnormal: {health['status']}")
            print(f"Cluster {self.es_url} health status: {health['status']}")
            return health["status"]
        except Exception as e:
            raise Exception(f"Unable to connect to cluster {self.es_url}: {e}")
    
    def get_latest_timestamp(self, timestamp_field, index="*"):
        """Get latest timestamp value from index. Usage: helper.get_latest_timestamp('updated_at')"""
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
        """Get start time of latest snapshot. Usage: helper.get_snapshot_start_time('my_repo')"""
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
        """Get earliest timestamp value from index. Usage: helper.get_earliest_timestamp('created_at')"""
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
