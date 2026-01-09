#!/usr/bin/env python3
"""
LogStashHelper - Logstash configuration and incremental sync execution helper class

Purpose:
- Set up and verify Logstash runtime environment (Java, Logstash installation path)
- Manage Logstash configuration file templates
- Execute time-range based incremental data synchronization
- Provide retry mechanism for handling sync failures
"""

import os
import json
import subprocess
import time
from pathlib import Path

from config import (
    TEMPLATE_PATH, JAVA_HOME, LS_HOME, LOG_FILE, SOURCE_ES, TARGET_ES
)


class LogStashHelper:
    def __init__(self, ls_home=LS_HOME, java_home=JAVA_HOME, 
                 template_path=TEMPLATE_PATH, source_es_endpoint=SOURCE_ES, 
                 target_es_endpoint=TARGET_ES, log_file=LOG_FILE):
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
        """Load and prepare Logstash config template with endpoint substitution."""
        try:
            base_config = f"/tmp/template_{os.getpid()}_.conf"
            # Read template and replace query
            with open(self.template_path, "r") as f:
                template = f.read()
            
            with open(base_config, "w") as f:
                f.write(template.replace("SOURCE_ES_ENDPOINT", self.source_es_endpoint).
                        replace("TARGET_ES_ENDPOINT", self.target_es_endpoint).
                        replace("LOG_FILE", self.log_file))
        except Exception as e:
            raise Exception(f"Failed to setup Logstash configuration template: {e}")

        return base_config

    def _setup_environment(self):
        """Set JAVA_HOME, PATH, LS_HOME environment variables."""
        os.environ["JAVA_HOME"] = self.java_home
        os.environ["PATH"] = f"{self.java_home}/bin:{os.environ['PATH']}"
        os.environ["LS_HOME"] = self.ls_home
    
    def _check_installation(self):
        """Verify Logstash, Java, and config template exist. Raises on failure."""
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
        """
        Run Logstash to sync docs in time range with retry.
        Usage: helper.run_incremental_sync('2024-01-01T00:00:00', '2024-01-02T00:00:00', 'updated_at')
        Returns True on success, raises on max retries exceeded.
        """
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
