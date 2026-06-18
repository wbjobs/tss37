"""
车队管理引擎 v3 — 多车辆分区状态管理 + 异常告警系统

架构：
  FleetStateManager
    ├─ partitions: Dict[vehicle_id, VehiclePartition]   # 每辆车独立分区
    │   ├─ analyzer: BehaviorAnalyzer                    # 独立的事件时间水印窗口
    │   ├─ latest_result: dict                            # 最近一次画像结果
    │   └─ last_alert_time: Dict[alert_type, timestamp]   # 告警冷却状态
    ├─ alert_engine: AlertEngine                         # 阈值检测 + 去重 + 分发
    └─ aggregation: 车队聚合统计（平均分、告警计数等）

参考 Flink KeyedStream + KeyedState 模型：
  - vehicle_id 作为 key，每个 key 独立维护状态
  - 数据按 key 路由到对应 partition，保证多租户隔离
  - 告警去重使用 sliding cool-down window，避免告警风暴
"""
import json
import math
import os
import sys
import time
import threading
from collections import deque
from typing import Dict, Any, List, Tuple, Optional

sys.path.insert(0, os.path.dirname(__file__))
from behavior_analyzer import BehaviorAnalyzer

try:
    from confluent_kafka import Producer as KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False


AGGRESSIVE_ALERT_THRESHOLD = float(os.getenv("AGGRESSIVE_ALERT", "75.0"))
FATIGUE_ALERT_THRESHOLD = float(os.getenv("FATIGUE_ALERT", "70.0"))
ALERT_COOLDOWN_SECONDS = int(os.getenv("ALERT_COOLDOWN", "60"))
KAFKA_ALERT_TOPIC = os.getenv("KAFKA_ALERT_TOPIC", "vehicle_alerts")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")


class VehiclePartition:
    """
    单车分区状态 — 对应 Flink KeyedState 的一个 keyed partition

    每个 vehicle_id 独立维护：
      - 事件时间水印窗口 (BehaviorAnalyzer)
      - 最近一次画像结果
      - 告警冷却时间戳（防止告警风暴）
      - 历史轨迹点
    """

    def __init__(self, vehicle_id: str, window_seconds: int = 300,
                 output_interval: int = 10, allowed_lateness: float = 5.0):
        self.vehicle_id = vehicle_id
        self.analyzer = BehaviorAnalyzer(
            window_seconds=window_seconds,
            output_interval=output_interval,
            allowed_lateness=allowed_lateness
        )
        self.latest_result: Optional[Dict] = None
        self.last_output_event_time: float = -float("inf")
        self.alert_cool_down: Dict[str, float] = {}
        self.trajectory: deque = deque(maxlen=500)
        self.first_seen: float = time.time()
        self.last_seen: float = time.time()
        self.record_count: int = 0

    def add_record(self, record: Dict) -> Tuple[Dict, bool]:
        """
        注入一条记录，返回 (分类结果, 是否触发了新的输出计算)
        """
        self.last_seen = time.time()
        self.record_count += 1

        gps = record.get("gps")
        if gps:
            self.trajectory.append({
                "timestamp": record["timestamp"],
                "lat": gps["lat"],
                "lon": gps["lon"]
            })

        result = self.analyzer.add_record(record)

        triggered = False
        if self.analyzer.should_output():
            self.latest_result = self.analyzer.get_result()
            self.latest_result["vehicle_id"] = self.vehicle_id
            self.last_output_event_time = self.latest_result.get("event_time_watermark", 0)
            triggered = True

        return result, triggered

    def get_trajectory(self, limit: int = 100) -> List[Dict]:
        if len(self.trajectory) <= limit:
            return list(self.trajectory)
        return list(self.trajectory)[-limit:]

    def get_status(self) -> Dict:
        result = self.latest_result
        return {
            "vehicle_id": self.vehicle_id,
            "online": time.time() - self.last_seen < 30,
            "record_count": self.record_count,
            "last_seen": round(self.last_seen, 2),
            "overall_score": result["overall_score"] if result else None,
            "aggressive_index": result["aggressive_index"] if result else None,
            "fatigue_index": result["fatigue_index"] if result else None,
            "economy_index": result["economy_index"] if result else None,
            "latest_gps": result["latest_vehicle"]["gps"] if result else None,
            "latest_speed": result["latest_vehicle"]["speed"] if result else None,
        }


class AlertEngine:
    """
    异常告警引擎

    功能：
      1. 阈值检测：激进指数/疲劳指数超过阈值
      2. 告警去重：同一车辆同一类型告警在 cool_down 秒内只发一次
      3. 双通道分发：
         - WebSocket 推送给前端（实时弹窗 + 高亮）
         - Kafka Topic (vehicle_alerts) 供下游消费

    告警结构：
      {
        "alert_id":       str,      # 唯一 ID
        "alert_type":     "aggressive" | "fatigue",
        "vehicle_id":     str,
        "timestamp":      float,    # 事件时间
        "severity":       "warning" | "critical",
        "index_value":    float,
        "threshold":      float,
        "message":        str,
        "vehicle_snapshot": {...}   # 当时的车辆状态快照
      }
    """

    def __init__(self, aggressive_threshold: float = AGGRESSIVE_ALERT_THRESHOLD,
                 fatigue_threshold: float = FATIGUE_ALERT_THRESHOLD,
                 cool_down: int = ALERT_COOLDOWN_SECONDS):
        self.aggressive_threshold = aggressive_threshold
        self.fatigue_threshold = fatigue_threshold
        self.cool_down = cool_down
        self.alerts: deque = deque(maxlen=500)
        self._kafka_producer = None
        self._lock = threading.Lock()

        if KAFKA_AVAILABLE:
            try:
                self._kafka_producer = KafkaProducer({"bootstrap.servers": KAFKA_BOOTSTRAP})
                print(f"[告警引擎] Kafka 告警通道就绪，主题: {KAFKA_ALERT_TOPIC}")
            except Exception as e:
                print(f"[告警引擎] Kafka 连接失败，仅使用 WebSocket 通道: {e}")
        else:
            print("[告警引擎] confluent_kafka 未安装，仅使用 WebSocket 告警通道")

    def _severity(self, value: float, threshold: float) -> str:
        ratio = value / threshold
        if ratio >= 1.3:
            return "critical"
        return "warning"

    def _should_emit(self, partition: VehiclePartition, alert_type: str) -> bool:
        now = time.time()
        last = partition.alert_cool_down.get(alert_type, 0)
        if now - last < self.cool_down:
            return False
        partition.alert_cool_down[alert_type] = now
        return True

    def _make_alert(self, partition: VehiclePartition, alert_type: str,
                    index_value: float, threshold: float) -> Dict:
        import uuid
        now = time.time()
        snapshot = partition.latest_result or {}
        if alert_type == "aggressive":
            msg = f"车辆 {partition.vehicle_id} 驾驶风格激进（激进指数 {index_value}），请注意安全"
        else:
            msg = f"车辆 {partition.vehicle_id} 疑似疲劳驾驶（疲劳指数 {index_value}），建议立即休息"

        return {
            "alert_id": str(uuid.uuid4()),
            "alert_type": alert_type,
            "vehicle_id": partition.vehicle_id,
            "timestamp": now * 1000,
            "severity": self._severity(index_value, threshold),
            "index_value": index_value,
            "threshold": threshold,
            "message": msg,
            "vehicle_snapshot": {
                "overall_score": snapshot.get("overall_score"),
                "aggressive_index": snapshot.get("aggressive_index"),
                "fatigue_index": snapshot.get("fatigue_index"),
                "economy_index": snapshot.get("economy_index"),
                "latest_vehicle": snapshot.get("latest_vehicle"),
            }
        }

    def check_and_emit(self, partition: VehiclePartition) -> List[Dict]:
        """
        检查分区是否触发告警，返回本次新生成的告警列表
        """
        result = partition.latest_result
        if not result:
            return []

        emitted = []
        with self._lock:
            agg = result["aggressive_index"]
            if agg >= self.aggressive_threshold and self._should_emit(partition, "aggressive"):
                alert = self._make_alert(partition, "aggressive", agg, self.aggressive_threshold)
                self.alerts.append(alert)
                emitted.append(alert)
                self._send_kafka(alert)

            fat = result["fatigue_index"]
            if fat >= self.fatigue_threshold and self._should_emit(partition, "fatigue"):
                alert = self._make_alert(partition, "fatigue", fat, self.fatigue_threshold)
                self.alerts.append(alert)
                emitted.append(alert)
                self._send_kafka(alert)

        return emitted

    def _send_kafka(self, alert: Dict):
        if not self._kafka_producer:
            return
        try:
            payload = json.dumps(alert, ensure_ascii=False).encode("utf-8")
            self._kafka_producer.produce(KAFKA_ALERT_TOPIC, value=payload)
            self._kafka_producer.poll(0)
        except Exception as e:
            print(f"[告警引擎] Kafka 发送失败: {e}")

    def get_recent_alerts(self, limit: int = 50) -> List[Dict]:
        with self._lock:
            if len(self.alerts) <= limit:
                return list(self.alerts)
            return list(self.alerts)[-limit:]


class FleetStateManager:
    """
    车队状态管理器 — 多分区 + 多租户数据隔离

    设计参考：
      - Flink KeyedStream: 按 vehicle_id 路由到独立的 KeyedState
      - Kafka Partition: 每个 vehicle_id 逻辑上对应一个分区
      - RocksDB State Backend: 内存 dict 存储，可扩展到外部存储

    特性：
      - 线程安全：_lock 保护 partitions 字典
      - 延迟创建：车辆首次出现时才创建分区
      - 分区隔离：每辆车的水印、窗口、告警完全独立
      - 车队聚合：实时计算车队级统计指标
    """

    def __init__(self, window_seconds: int = 300, output_interval: int = 10,
                 allowed_lateness: float = 5.0):
        self.window_seconds = window_seconds
        self.output_interval = output_interval
        self.allowed_lateness = allowed_lateness
        self.partitions: Dict[str, VehiclePartition] = {}
        self.alert_engine = AlertEngine()
        self._lock = threading.RLock()
        self._vehicle_names = self._generate_vehicle_names()

    @staticmethod
    def _generate_vehicle_names() -> Dict[str, str]:
        """为车辆生成友好的显示名称"""
        brands = ["京A", "沪B", "粤C", "浙D", "苏E", "川F", "鲁G", "冀H"]
        names = {}
        for i in range(50):
            vid = f"veh-{i+1:03d}"
            plate = f"{brands[i % len(brands)]}·{10000 + i}"
            names[vid] = plate
        return names

    def get_vehicle_name(self, vehicle_id: str) -> str:
        return self._vehicle_names.get(vehicle_id, vehicle_id)

    def _get_or_create_partition(self, vehicle_id: str) -> VehiclePartition:
        with self._lock:
            if vehicle_id not in self.partitions:
                self.partitions[vehicle_id] = VehiclePartition(
                    vehicle_id=vehicle_id,
                    window_seconds=self.window_seconds,
                    output_interval=self.output_interval,
                    allowed_lateness=self.allowed_lateness
                )
            return self.partitions[vehicle_id]

    def add_record(self, record: Dict) -> Dict[str, Any]:
        """
        注入一条带 vehicle_id 的 CAN 记录。

        返回：
          {
            "vehicle_id": str,
            "classification": "on_time"|"late"|"drop",
            "output_triggered": bool,
            "new_alerts": [alert...],
            "vehicle_result": dict | None  # 如果触发了输出则为最新结果
          }
        """
        vehicle_id = record.get("vehicle_id")
        if not vehicle_id:
            raise ValueError("Record missing 'vehicle_id' field")

        partition = self._get_or_create_partition(vehicle_id)
        classification, triggered = partition.add_record(record)

        new_alerts = []
        vehicle_result = None
        if triggered:
            vehicle_result = partition.latest_result
            new_alerts = self.alert_engine.check_and_emit(partition)

        return {
            "vehicle_id": vehicle_id,
            "classification": classification["status"],
            "output_triggered": triggered,
            "new_alerts": new_alerts,
            "vehicle_result": vehicle_result,
        }

    def get_vehicle_result(self, vehicle_id: str) -> Optional[Dict]:
        with self._lock:
            p = self.partitions.get(vehicle_id)
            return p.latest_result if p else None

    def get_vehicle_trajectory(self, vehicle_id: str, limit: int = 100) -> List[Dict]:
        with self._lock:
            p = self.partitions.get(vehicle_id)
            return p.get_trajectory(limit) if p else []

    def get_all_vehicle_status(self) -> List[Dict]:
        with self._lock:
            return [
                {
                    **p.get_status(),
                    "vehicle_name": self.get_vehicle_name(p.vehicle_id)
                }
                for p in self.partitions.values()
            ]

    def get_fleet_summary(self) -> Dict[str, Any]:
        """
        车队聚合统计：在线数、平均分、告警车辆等

        参考 Flink 的 GlobalWindow + Reduce 聚合
        """
        with self._lock:
            vehicles = list(self.partitions.values())

        total = len(vehicles)
        if total == 0:
            return {
                "total_vehicles": 0,
                "online_vehicles": 0,
                "avg_overall": 0,
                "avg_aggressive": 0,
                "avg_fatigue": 0,
                "avg_economy": 0,
                "alerting_count": 0,
                "total_records": 0,
            }

        online = 0
        overall_scores = []
        aggressive_scores = []
        fatigue_scores = []
        economy_scores = []
        alerting = 0
        total_records = 0

        for p in vehicles:
            total_records += p.record_count
            if time.time() - p.last_seen < 30:
                online += 1
            r = p.latest_result
            if r:
                overall_scores.append(r["overall_score"])
                aggressive_scores.append(r["aggressive_index"])
                fatigue_scores.append(r["fatigue_index"])
                economy_scores.append(r["economy_index"])
                if (r["aggressive_index"] >= self.alert_engine.aggressive_threshold or
                        r["fatigue_index"] >= self.alert_engine.fatigue_threshold):
                    alerting += 1

        def avg(lst):
            return round(sum(lst) / len(lst), 1) if lst else 0

        return {
            "total_vehicles": total,
            "online_vehicles": online,
            "avg_overall": avg(overall_scores),
            "avg_aggressive": avg(aggressive_scores),
            "avg_fatigue": avg(fatigue_scores),
            "avg_economy": avg(economy_scores),
            "alerting_count": alerting,
            "total_records": total_records,
            "thresholds": {
                "aggressive": self.alert_engine.aggressive_threshold,
                "fatigue": self.alert_engine.fatigue_threshold,
            },
        }

    def get_recent_alerts(self, limit: int = 50) -> List[Dict]:
        return self.alert_engine.get_recent_alerts(limit)
