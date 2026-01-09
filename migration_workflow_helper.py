#!/usr/bin/env python3
"""
OpenSearchMigrationWorkflowHelper - OpenSearch migration workflow control helper class

Purpose:
- Coordinate incremental data migration between source and target clusters
- Manage migration progress (save/restore checkpoints)
- Auto-calculate batch size, dynamically adjust based on data gap
- Handle near real-time sync scenarios, support final cutover confirmation
- Provide complete migration workflow control
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

from config import TIMESTAMP_FIELD, SNAPSHOT_REPO, PROGRESS_FILE, LOG_FILE, INDEX_NAME


class OpenSearchMigrationWorkflowHelper:
    def __init__(self, source_helper, target_helper, logstash_helper, 
                 timestamp_field=TIMESTAMP_FIELD, snapshot_repo=SNAPSHOT_REPO, 
                 progress_file=PROGRESS_FILE, log_file=LOG_FILE, index_name=INDEX_NAME):
        self.source_helper = source_helper
        self.target_helper = target_helper
        self.timestamp_field = timestamp_field
        self.snapshot_repo = snapshot_repo
        self.logstash_helper = logstash_helper
        self.progress_file = progress_file
        self.log_file = log_file
        self.index_name = index_name

    def _log(self, msg):
        """Log message to console and file with timestamp."""
        timestamp = datetime.now().strftime("%a %b %d %H:%M:%S UTC %Y")
        log_msg = f"{timestamp}: {msg}"
        print(log_msg)
        try:
            with open(self.log_file, "a") as f:
                f.write(log_msg + "\n")
        except Exception as e:
            print(f"Failed to write to log file: {e}")

    @staticmethod
    def _time_diff_minutes(time1, time2):
        """Calculate difference between two ISO timestamps in minutes."""
        try:
            dt1 = datetime.fromisoformat(time1.replace("Z", "+00:00").replace("+00:00", "+00:00") if "Z" in time1 or "+" in time1 else time1 + "+00:00")
            dt2 = datetime.fromisoformat(time2.replace("Z", "+00:00").replace("+00:00", "+00:00") if "Z" in time2 or "+" in time2 else time2 + "+00:00")
            return int((dt2 - dt1).total_seconds() / 60)
        except Exception as e:
            raise Exception(f"Time difference calculation failed: {e}")
    
    def get_gap_minutes(self, start_time):
        """Get minutes gap between start_time and source cluster latest. Returns (gap, latest_ts)."""
        source_latest = self.source_helper.get_latest_timestamp(self.timestamp_field, self.index_name)
        gap_minutes = self._time_diff_minutes(start_time, source_latest)
        return gap_minutes, source_latest
    
    def get_next_time(self, start_time, gap_minutes, source_latest):
        """Calculate next batch end time based on gap size. Larger gap = bigger batch."""
        if gap_minutes > 1440:
            batch_hours = 12
        elif gap_minutes > 360:
            batch_hours = 6
        else:
            batch_hours = 1
    
        dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
        next_time = (dt + timedelta(hours=batch_hours)).isoformat()
        
        if next_time > source_latest:
            next_time = source_latest
        
        return next_time

    def get_start_time(self):
        """Determine migration start time from progress file, snapshot, or target cluster."""
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
                start_time = self.target_helper.get_latest_timestamp(self.timestamp_field,self.index_name)
                if not start_time:
                    self._log("Target cluster has no data, using source cluster earliest time")
                    start_time = self.source_helper.get_earliest_timestamp(self.timestamp_field, self.index_name)
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
        """Prompt user to confirm start time. Exits if not confirmed."""
        print("=" * 60)
        self._log(f"Incremental migration start time: {start_time}")
        print("=" * 60)
        confirm = input("Confirm using this time as incremental migration start point? (y/N): ")
        if confirm.lower() not in ['y', 'yes']:
            print("Migration cancelled by user")
            sys.exit(0)
        self.save_progress(start_time)

    def handle_near_realtime(self, start_time):
        """Handle final sync when gap <= 5 min. Prompts for write-stop confirmation."""
        print("=" * 50)
        gap_minutes, source_latest = self.get_gap_minutes(start_time)
        self._log(f"Incremental migration is near real-time! Current gap: {gap_minutes} minutes")
        print("Please perform the following steps:")
        print("1. Stop business write operations")
        print("2. Enter 'y' to execute final sync...confirm sync completion")
        print("3. Switch application endpoint to target cluster, resume writes")
        print("=" * 50)

        # Wait for user confirmation to stop writes
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
        """Save current progress timestamp to file for resume capability."""
        with open(self.progress_file, "w") as f:
            f.write(start_time)
        self._log(f"Progress saved: {start_time}")

    def run_migration(self):
        """Execute full migration workflow: get start time, batch sync until near real-time, then final cutover."""
        import time
        
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
