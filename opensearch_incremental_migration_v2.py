#!/usr/bin/env python3
import os
import sys
import json
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse
import time

# 配置
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
        """创建OpenSearch客户端"""
        try:
            from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth
            import boto3
        except ImportError:
            raise Exception("请安装opensearch-py和boto3: pip install opensearch-py boto3")
        
        parsed_url = urlparse(self.es_url)
        host = parsed_url.hostname
        port = parsed_url.port or (443 if parsed_url.scheme == 'https' else 9200)
        use_ssl = parsed_url.scheme == 'https'
        
        # 检测是否为AWS OpenSearch
        is_aws_opensearch = 'es.amazonaws.com' in host
        
        if is_aws_opensearch:
            # AWS OpenSearch连接
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
                print(f"使用AWS IAM认证连接到: {host}")
                return client
            except Exception as e:
                print(f"AWS IAM认证失败: {e}")
                if self.auth:
                    print("尝试使用用户名密码认证...")
                else:
                    raise Exception("AWS OpenSearch连接失败，请检查IAM权限或提供用户名密码")
        
        # 标准OpenSearch连接或AWS备用连接
        client = OpenSearch(
            hosts=[{'host': host, 'port': port}],
            http_auth=self.auth,
            http_compress=True,
            use_ssl=use_ssl,
            verify_certs=use_ssl,
            ssl_assert_hostname=False,
            ssl_show_warn=False
        )
        
        auth_type = "用户名密码" if self.auth else "无认证"
        print(f"使用{auth_type}连接到: {host}")
        return client
    
    def check_cluster_health(self):
        """检查集群健康状态"""
        try:
            health = self.client.cluster.health(timeout=10)
            if health["status"] == "red":
                raise Exception(f"集群状态异常: {health['status']}")
            print(f"集群 {self.es_url} 健康状态: {health['status']}")
            return health["status"]
        except Exception as e:
            raise Exception(f"无法连接到集群 {self.es_url}: {e}")
    
    def get_latest_timestamp(self, timestamp_field, index="*"):
        """获取所有索引中最新文档时间戳，带连接检查"""
        try:
            # 先检查集群状态
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
            raise Exception(f"获取最新时间戳失败: {e}")
    
    def get_snapshot_start_time(self, snapshot_repo):
        """获取最新快照开始时间"""
        try:
            response = self.client.snapshot.get(repository=snapshot_repo, snapshot="_all")
            snapshots = response["snapshots"]
            if snapshots:
                latest_snapshot = max(snapshots, key=lambda x: x["start_time_in_millis"])
                return latest_snapshot["start_time"]
            return None
        except Exception as e:
            print(f"警告: 获取快照时间失败: {e}")
            return None
    
    def get_earliest_timestamp(self, timestamp_field, index="*"):
        """获取所有索引中最早文档时间戳"""
        try:
            # 先检查集群状态
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
            raise Exception(f"获取最早时间戳失败: {e}")

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
        """设置Logstash配置文件路径"""
        try:
            base_config = f"/tmp/template_{os.getpid()}_.conf"
            # 读取模板并替换查询
            with open(TEMPLATE_PATH, "r") as f:
                template = f.read()
            
            with open(base_config, "w") as f:
                f.write(template.replace("SOURCE_ES_ENDPOINT", self.source_es_endpoint).
                        replace("TARGET_ES_ENDPOINT", self.target_es_endpoint).
                        replace("LOG_FILE", self.log_file))
        except Exception as e:
            raise Exception(f"设置Logstash配置模板失败: {e}")

        return base_config

    def _setup_environment(self):
        """设置环境变量"""
        os.environ["JAVA_HOME"] = self.java_home
        os.environ["PATH"] = f"{self.java_home}/bin:{os.environ['PATH']}"
        os.environ["LS_HOME"] = self.ls_home
    
    def _check_installation(self):
        """检查本地logstash安装情况"""
        try:
            # 检查Logstash目录
            if not Path(self.ls_home).exists():
                raise Exception(f"Logstash目录不存在: {self.ls_home}")
            
            # 检查Logstash可执行文件
            logstash_bin = Path(self.ls_home) / "bin" / "logstash"
            if not logstash_bin.exists():
                raise Exception(f"Logstash可执行文件不存在: {logstash_bin}")
            
            # 检查Java
            java_bin = Path(self.java_home) / "bin" / "java"
            if not java_bin.exists():
                raise Exception(f"Java可执行文件不存在: {java_bin}")
            
            # 检查配置模板
            if not Path(self.template_path).exists():
                raise Exception(f"Logstash配置模板不存在: {self.template_path}")
            
            print(f"Logstash安装检查通过: {self.ls_home}")
            
        except Exception as e:
            raise Exception(f"Logstash安装检查失败: {e}")
    
    def run_incremental_sync(self, start_time, end_time, timestamp_field, max_retries=3):
        """执行增量同步，带重试机制"""
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
                
                # 读取模板并替换查询
                with open(self.base_config, "r") as f:
                    template = f.read()
                
                with open(batch_config, "w") as f:
                    f.write(template.replace("BATCH_QUERY_PLACEHOLDER", query))
                
                print(f"增量同步 {start_time} -> {end_time} (尝试 {attempt + 1}/{max_retries})")
                
                result = subprocess.run(
                    [f"{self.ls_home}/bin/logstash", "-f", batch_config],
                    capture_output=True, text=True, timeout=3600
                )
                
                if result.returncode == 0:
                    print(f"增量同步成功 (尝试 {attempt + 1})")
                    return True
                else:
                    error_msg = f"Logstash执行失败 (尝试 {attempt + 1}): 返回码={result.returncode}"
                    if result.stderr:
                        error_msg += f", 错误输出: {result.stderr[-500:]}"  # 只显示最后500字符
                    print(error_msg)
                    
                    if attempt < max_retries - 1:
                        wait_time = (attempt + 1) * 30  # 递增等待时间
                        print(f"等待 {wait_time} 秒后重试...")
                        time.sleep(wait_time)
                    else:
                        raise Exception(f"Logstash执行失败，已重试 {max_retries} 次: {result.stderr}")
                
            except subprocess.TimeoutExpired:
                error_msg = f"Logstash执行超时 (尝试 {attempt + 1})"
                print(error_msg)
                if attempt < max_retries - 1:
                    print("超时后重试...")
                else:
                    raise Exception("Logstash执行超时，已达到最大重试次数")
            except Exception as e:
                error_msg = f"增量同步异常 (尝试 {attempt + 1}): {e}"
                print(error_msg)
                if attempt < max_retries - 1:
                    print("异常后重试...")
                else:
                    raise Exception(f"增量同步失败，已重试 {max_retries} 次: {e}")
            finally:
                # 清理临时配置文件
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
        """同时输出到文件和控制台"""
        timestamp = datetime.now().strftime("%a %b %d %H:%M:%S UTC %Y")
        log_msg = f"{timestamp}: {msg}"
        print(log_msg)
        try:
            with open(log_file, "a") as f:
                f.write(log_msg + "\n")
        except Exception as e:
            print(f"写入日志文件失败: {e}")

    @staticmethod
    def _time_diff_minutes(time1, time2):
        """计算时间差（分钟）"""
        try:
            dt1 = datetime.fromisoformat(time1.replace("Z", "+00:00").replace("+00:00", "+00:00") if "Z" in time1 or "+" in time1 else time1 + "+00:00")
            dt2 = datetime.fromisoformat(time2.replace("Z", "+00:00").replace("+00:00", "+00:00") if "Z" in time2 or "+" in time2 else time2 + "+00:00")
            return int((dt2 - dt1).total_seconds() / 60)
        except Exception as e:
            raise Exception(f"时间差计算失败: {e}")
    
    def get_gap_minutes(self, start_time):
        """获取两个时间点的差距（分钟）"""
        source_latest = self.source_helper.get_latest_timestamp(self.timestamp_field)
        gap_minutes = self._time_diff_minutes(start_time, source_latest)
        return gap_minutes, source_latest
    
    def get_next_time(self, start_time, gap_minutes, source_latest):

        """根据时间差计算批次大小"""
        if gap_minutes > 1440:
            batch_hours = 12
        elif gap_minutes > 360:
            batch_hours = 6
        else:
            batch_hours = 1
    
        """计算下一个时间点"""
        dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
        next_time = (dt + timedelta(hours=batch_hours)).isoformat()
        
        if next_time > source_latest:
            next_time = source_latest
        
        return next_time

    def get_start_time(self):
        """获取迁移起始时间"""
        if Path(self.progress_file).exists():
            with open(self.progress_file, "r") as f:
                start_time = f.read().strip()
            self._log(f"从上次进度继续: {start_time}")
            return start_time

        self._log("正在获取增量迁移起始时间...")

        try:
            snapshot_time = self.source_helper.get_snapshot_start_time(self.snapshot_repo)
            if snapshot_time:
                start_time = snapshot_time
                self._log(f"使用源集群快照时间作为起始点: {start_time}")
            else:
                self._log("未找到源集群快照时间，尝试获取目标集群最新数据时间...")
                start_time = self.target_helper.get_latest_timestamp(self.timestamp_field)
                if not start_time:
                    self._log("目标集群无数据，使用源集群最早时间")
                    start_time = self.source_helper.get_earliest_timestamp(self.timestamp_field)
                    if not start_time:
                        self._log("错误: 无法获取任何时间戳，迁移暂停")
                        sys.exit(1)
                    self._log(f"使用源集群最早时间作为起始点: {start_time}")
                else:
                    self._log(f"使用目标集群最新时间作为起始点: {start_time}")
        except Exception as e:
            self._log(f"获取起始时间失败: {e}")
            sys.exit(1)
        
        return start_time

    def confirm_start_time(self, start_time):
        """用户确认起始时间"""
        print("=" * 60)
        self._log(f"增量迁移起始时间: {start_time}")
        print("=" * 60)
        confirm = input("确认使用此时间作为增量迁移起始点? (y/N): ")
        if confirm.lower() not in ['y', 'yes']:
            print("用户取消迁移")
            sys.exit(0)
        self.save_progress(start_time)

    def handle_near_realtime(self, start_time):
        """处理接近实时的情况"""
        print("=" * 50)
        gap_minutes, source_latest = self.get_gap_minutes(start_time)
        self._log(f"增量迁移已接近实时！当前差距: {gap_minutes}分钟")
        print("请执行以下步骤：")
        print("1. 停止业务写入操作")
        print("2. 输入y执行最后一次同步...确认同步完成")
        print("3. 切换应用endpoint到目标集群，恢复写")
        print("=" * 50)

        # 等待用户确认停写
        # 等待用户输入y，否则一直保持等待
        stop_confirm = input("确认已完成停写? (y/N): ")
        while stop_confirm.lower() not in ['y', 'yes']:
            stop_confirm = input("未收到用户输入y，请确认是否完成停写(y/N): ")

        # 再次检查时间差异
        gap_minutes, source_latest = self.get_gap_minutes(start_time)
        if gap_minutes > 6:
            self._log("未及时停写，当前数据差异仍然较大，继续增量同步")
            return False
        else:
            # 最后一次同步
            self._log("执行最后一次增量同步...")
            final_latest = datetime.now().isoformat()
            
            if self.logstash_helper.run_incremental_sync(start_time, final_latest, self.timestamp_field):
                self._log("增量迁移完成！")
                Path(self.progress_file).unlink(missing_ok=True)
                sys.exit(0)
            else:
                self._log("最后同步失败！")
                sys.exit(1)

    def save_progress(self, start_time):
        """保存进度"""
        with open(self.progress_file, "w") as f:
            f.write(start_time)
        self._log(f"已保存进度: {start_time}")

    def run_migration(self):
        """执行完整的迁移流程"""
        self._log("开始快照后增量迁移...")
        
        # 获取起始时间
        start_time = self.get_start_time()
        self.confirm_start_time(start_time)
        
        # 循环追踪直到接近实时
        while True:
            try:
                gap_minutes, source_latest = self.get_gap_minutes(start_time)
                
                self._log(f"当前进度: {start_time}, 源集群最新: {source_latest}, 差距: {gap_minutes}分钟")
                
                if gap_minutes <= 5:
                    if self.handle_near_realtime(start_time):
                        break
                
                # 计算批次大小
                next_time = self.get_next_time(start_time, gap_minutes, source_latest)
                
                # 执行增量同步
                if self.logstash_helper.run_incremental_sync(start_time, next_time, self.timestamp_field):
                    start_time = next_time
                    self.save_progress(start_time)
                    time.sleep(5)
                else:
                    self._log("增量同步失败")
                    sys.exit(1)
                    
            except Exception as e:
                self._log(f"迁移过程中发生错误: {e}")
                sys.exit(1)


def main():
    try:
        # 初始化
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
        
        # 执行迁移
        migration_workflow_helper.run_migration()
                
    except KeyboardInterrupt:
        print("\n迁移已中断")
        sys.exit(1)
    except Exception as e:
        print(f"迁移初始化失败: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()