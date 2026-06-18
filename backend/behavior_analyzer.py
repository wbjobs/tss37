"""
驾驶员行为画像计算引擎 v2 — 事件时间语义 + 水印(Watermark)机制

架构参考 Flink 的 BoundedOutOfOrdernessWatermarks + 滑动窗口模型：
  - Watermark = max_event_time_seen - allowed_lateness
  - 窗口范围: [watermark - window_seconds, watermark]
  - 迟到数据: event_time < watermark 但 >= watermark - window_seconds 的记录被接受为"迟到数据"
  - 丢弃数据: event_time < watermark - window_seconds 的记录被彻底丢弃
  - 输出触发: 水印推进超过 last_output_watermark + output_interval 时触发

替代方案说明：若使用 Faust 库，可用 faust.Stream + app.Table 实现等价逻辑；
若使用 Flink DataStream API，可直接用 KeyedProcessFunction + EventTimeTimer。
本实现以纯 Python 复现核心语义，无外部流处理框架依赖。
"""
import bisect
import math
import time
from typing import Dict, Any, Tuple, List, Optional


WINDOW_SECONDS = 300
OUTPUT_INTERVAL = 10
ALLOWED_LATENESS = 5.0

ACCELERATION_THRESHOLD = 2.5
DECELERATION_THRESHOLD = -2.5
STEER_RATE_THRESHOLD = 80

FATIGUE_STEER_STABLE_THRESHOLD = 3.0
FATIGUE_STEER_STABLE_SECONDS = 8.0
CURVATURE_CHANGE_THRESHOLD = 0.002

ECONOMY_IDLE_RPM_THRESHOLD = 1200
ECONOMY_HIGH_RPM_THRESHOLD = 2500
ECONOMY_OPTIMAL_SPEED_MIN = 40
ECONOMY_OPTIMAL_SPEED_MAX = 90


class Watermark:
    """
    事件时间水印跟踪器（参考 Flink BoundedOutOfOrdernessWatermarks）

    核心语义：
      watermark = max_event_time - allowed_lateness
      水印单调递增，不会回退。
      含义：当水印推进到 T 时，我们认为事件时间 <= T 的数据已经"基本到齐"，
            仅有最多 allowed_lateness 秒的迟到数据仍可能到来。
    """

    def __init__(self, allowed_lateness: float = ALLOWED_LATENESS):
        self.allowed_lateness = allowed_lateness
        self.max_event_time: float = -float("inf")
        self.current_watermark: float = -float("inf")
        self.late_accepted_count: int = 0
        self.dropped_count: int = 0

    def update(self, event_time: float) -> float:
        """
        根据新到达的事件时间更新水印。
        返回更新后的水印值。
        """
        if event_time > self.max_event_time:
            self.max_event_time = event_time
            new_wm = self.max_event_time - self.allowed_lateness
            if new_wm > self.current_watermark:
                self.current_watermark = new_wm
        return self.current_watermark

    def classify(self, event_time: float, window_start: float) -> str:
        """
        对到达的记录进行分类：
          - "on_time" : event_time >= watermark （正常数据）
          - "late"    : watermark > event_time >= window_start （迟到但仍在窗口内）
          - "drop"    : event_time < window_start （太迟，无法纳入任何当前窗口）
        """
        if event_time >= self.current_watermark:
            return "on_time"
        elif event_time >= window_start:
            self.late_accepted_count += 1
            return "late"
        else:
            self.dropped_count += 1
            return "drop"

    def get_stats(self) -> Dict[str, Any]:
        return {
            "max_event_time": round(self.max_event_time, 3),
            "watermark": round(self.current_watermark, 3),
            "allowed_lateness": self.allowed_lateness,
            "late_accepted_count": self.late_accepted_count,
            "dropped_count": self.dropped_count
        }


class EventTimeSortedBuffer:
    """
    事件时间有序缓冲区

    使用 bisect.insort 按事件时间排序插入，保证窗口计算时数据是有序的。
    这样即使消息乱序到达，事件检测（加速度、转向角等）始终基于
    事件时间相邻的记录计算，而非到达顺序相邻的记录。
    """

    def __init__(self):
        self._records: List[Dict] = []
        self._times: List[float] = []

    def insert(self, record: Dict) -> None:
        event_time = record["timestamp"] / 1000.0
        idx = bisect.bisect_right(self._times, event_time)
        self._records.insert(idx, record)
        self._times.insert(idx, event_time)

    def evict_before(self, cutoff_time: float) -> int:
        idx = bisect.bisect_left(self._times, cutoff_time)
        evicted = idx
        self._records = self._records[idx:]
        self._times = self._times[idx:]
        return evicted

    def get_all(self) -> List[Dict]:
        return self._records

    def __len__(self) -> int:
        return len(self._records)

    def get_time_range(self) -> Tuple[float, float]:
        if not self._times:
            return 0.0, 0.0
        return self._times[0], self._times[-1]


class BehaviorAnalyzer:
    """
    驾驶员行为分析器 v2 — 基于事件时间 + 水印的滑动窗口

    数据流：
      1. add_record() 接收原始 CAN 数据
      2. Watermark 判断记录是 on_time / late / drop
      3. 有序缓冲区按事件时间排序存储
      4. should_output() 基于水印推进判断是否触发计算
      5. compute_*() 方法在有序数据上重新检测事件并计算指数
    """

    def __init__(
        self,
        window_seconds: int = WINDOW_SECONDS,
        output_interval: int = OUTPUT_INTERVAL,
        allowed_lateness: float = ALLOWED_LATENESS
    ):
        self.window_seconds = window_seconds
        self.output_interval = output_interval
        self.watermark = Watermark(allowed_lateness)
        self.buffer = EventTimeSortedBuffer()
        self.last_output_watermark: float = -float("inf")

    def add_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        添加一条 CAN 数据记录。

        返回该记录的处理结果：
          {"status": "on_time"|"late"|"drop", "watermark": float}
        """
        event_time = record["timestamp"] / 1000.0

        wm = self.watermark.update(event_time)

        window_start = wm - self.window_seconds
        classification = self.watermark.classify(event_time, window_start)

        result = {
            "status": classification,
            "watermark": round(wm, 3),
            "event_time": round(event_time, 3)
        }

        if classification == "drop":
            return result

        self.buffer.insert(record)

        self._evict(wm)

        return result

    def _evict(self, watermark: float) -> None:
        cutoff = watermark - self.window_seconds
        self.buffer.evict_before(cutoff)

    def should_output(self) -> bool:
        """
        基于水印推进判断是否到达输出间隔。

        当水印推进到 last_output_watermark + output_interval 时触发。
        这保证了输出节奏由事件时间驱动，而非处理时间。
        """
        wm = self.watermark.current_watermark
        if wm - self.last_output_watermark >= self.output_interval:
            self.last_output_watermark = (
                math.floor(wm / self.output_interval) * self.output_interval
            )
            return True
        return False

    def _detect_events_on_sorted(self, records: List[Dict]) -> Dict[str, List[float]]:
        """
        在事件时间有序的记录上重新检测所有驾驶事件。

        这是 v2 的核心改进：事件检测不再基于到达顺序，
        而是基于事件时间相邻的记录，确保加速度/角速度计算正确。
        """
        accel_events = []
        decel_events = []
        steer_events = []

        for i in range(1, len(records)):
            prev = records[i - 1]
            curr = records[i]
            dt = (curr["timestamp"] - prev["timestamp"]) / 1000.0
            if dt <= 0:
                continue

            dv = (curr["speed"] - prev["speed"]) / 3.6
            accel = dv / dt
            curr_s = curr["timestamp"] / 1000.0

            if accel >= ACCELERATION_THRESHOLD:
                accel_events.append(curr_s)
            if accel <= DECELERATION_THRESHOLD:
                decel_events.append(curr_s)

            ds = abs(curr["steering"] - prev["steering"])
            steer_rate = ds / dt
            if steer_rate >= STEER_RATE_THRESHOLD:
                steer_events.append(curr_s)

        return {
            "accel": accel_events,
            "decel": decel_events,
            "steer": steer_events
        }

    def compute_aggressive_index(self) -> Tuple[float, Dict]:
        records = self.buffer.get_all()
        if len(records) < 5:
            return 30.0, {"accel_count": 0, "decel_count": 0, "steer_count": 0}

        events = self._detect_events_on_sorted(records)
        window_duration = self.window_seconds / 60.0

        accel_freq = len(events["accel"]) / window_duration
        decel_freq = len(events["decel"]) / window_duration
        steer_freq = len(events["steer"]) / window_duration

        accel_score = min(100.0, accel_freq * 15.0)
        decel_score = min(100.0, decel_freq * 15.0)
        steer_score = min(100.0, steer_freq * 10.0)

        index = accel_score * 0.35 + decel_score * 0.35 + steer_score * 0.30

        return round(min(100.0, index), 1), {
            "accel_count": len(events["accel"]),
            "decel_count": len(events["decel"]),
            "steer_count": len(events["steer"])
        }

    def compute_fatigue_index(self) -> Tuple[float, Dict]:
        records = self.buffer.get_all()
        if len(records) < 20:
            return 20.0, {"stable_segments": 0, "curvature_events": 0}

        stable_segments = 0
        current_stable_start = None
        curvature_events = 0

        for i in range(1, len(records)):
            dt = (records[i]["timestamp"] - records[i - 1]["timestamp"]) / 1000.0
            if dt <= 0:
                continue
            steer_diff = abs(records[i]["steering"] - records[i - 1]["steering"])

            if steer_diff <= FATIGUE_STEER_STABLE_THRESHOLD:
                if current_stable_start is None:
                    current_stable_start = records[i]["timestamp"] / 1000.0
                else:
                    duration = records[i]["timestamp"] / 1000.0 - current_stable_start
                    if duration >= FATIGUE_STEER_STABLE_SECONDS:
                        stable_segments += 1
                        current_stable_start = None
            else:
                current_stable_start = None

        for i in range(3, len(records)):
            p1 = records[i - 3]["gps"]
            p2 = records[i - 2]["gps"]
            p3 = records[i - 1]["gps"]
            p4 = records[i]["gps"]
            c1 = self._calc_curvature(p1, p2, p3)
            c2 = self._calc_curvature(p2, p3, p4)
            if abs(c2 - c1) >= CURVATURE_CHANGE_THRESHOLD:
                curvature_events += 1

        window_duration = self.window_seconds / 60.0
        stable_score = min(100.0, stable_segments / window_duration * 25.0)
        curvature_score = min(100.0, curvature_events / window_duration * 20.0)
        index = stable_score * 0.6 + curvature_score * 0.4

        return round(min(100.0, index), 1), {
            "stable_segments": stable_segments,
            "curvature_events": curvature_events
        }

    @staticmethod
    def _calc_curvature(p1: Dict, p2: Dict, p3: Dict) -> float:
        x1, y1 = p1["lon"], p1["lat"]
        x2, y2 = p2["lon"], p2["lat"]
        x3, y3 = p3["lon"], p3["lat"]
        a = math.hypot(x2 - x1, y2 - y1)
        b = math.hypot(x3 - x2, y3 - y2)
        c = math.hypot(x3 - x1, y3 - y1)
        if a * b * c == 0:
            return 0.0
        area = abs((x2 - x1) * (y3 - y1) - (x3 - x1) * (y2 - y1)) / 2.0
        return 4 * area / (a * b * c)

    def compute_economy_index(self) -> Tuple[float, Dict]:
        records = self.buffer.get_all()
        if len(records) < 10:
            return 60.0, {"avg_fuel": 0.0, "coast_ratio": 0.0, "idle_ratio": 0.0}

        total_fuel = 0.0
        coast_count = 0
        idle_count = 0
        high_rpm_count = 0
        optimal_speed_count = 0

        for r in records:
            speed = r["speed"]
            rpm = r["rpm"]
            throttle = r["throttle"]

            fuel_rate = self._estimate_fuel_rate(speed, rpm, throttle)
            total_fuel += fuel_rate

            if speed > 20 and throttle < 3:
                coast_count += 1
            if speed < 2 and rpm > ECONOMY_IDLE_RPM_THRESHOLD:
                idle_count += 1
            if rpm > ECONOMY_HIGH_RPM_THRESHOLD:
                high_rpm_count += 1
            if ECONOMY_OPTIMAL_SPEED_MIN <= speed <= ECONOMY_OPTIMAL_SPEED_MAX:
                optimal_speed_count += 1

        n = len(records)
        avg_fuel = total_fuel / n if n > 0 else 0
        coast_ratio = coast_count / n
        idle_ratio = idle_count / n
        high_rpm_ratio = high_rpm_count / n
        optimal_ratio = optimal_speed_count / n

        fuel_score = max(0.0, 100.0 - avg_fuel * 8.0)
        coast_score = min(100.0, coast_ratio * 300.0)
        idle_penalty = idle_ratio * 150.0
        high_rpm_penalty = high_rpm_ratio * 120.0
        optimal_bonus = optimal_ratio * 40.0

        index = fuel_score * 0.4 + coast_score * 0.25 + optimal_bonus + (100 - idle_penalty) * 0.2 + (100 - high_rpm_penalty) * 0.15
        index = max(0.0, min(100.0, index))

        return round(index, 1), {
            "avg_fuel": round(avg_fuel, 2),
            "coast_ratio": round(coast_ratio, 3),
            "idle_ratio": round(idle_ratio, 3)
        }

    @staticmethod
    def _estimate_fuel_rate(speed: float, rpm: float, throttle: float) -> float:
        if speed < 1:
            return 2.0 + rpm / 800.0
        base_fuel = 4.0 + (rpm - 1000) / 500.0
        throttle_factor = 1.0 + throttle / 150.0
        if speed < 20:
            speed_factor = 1.8
        elif 20 <= speed <= 90:
            speed_factor = 1.0
        else:
            speed_factor = 1.0 + (speed - 90) / 30.0
        return max(1.0, base_fuel * throttle_factor * speed_factor)

    def generate_summary(self, aggressive: float, fatigue: float, economy: float) -> str:
        tips = []

        if aggressive >= 70:
            tips.append("驾驶风格偏激进，急加速急刹车频繁，请注意行车安全")
        elif aggressive >= 40:
            tips.append("驾驶较为平稳，偶尔有急操作")
        else:
            tips.append("驾驶风格温和")

        if fatigue >= 70:
            tips.append("检测到疲劳驾驶特征，建议立即停车休息")
        elif fatigue >= 45:
            tips.append("注意力略有下降，请注意保持清醒")
        else:
            tips.append("精神状态良好")

        if economy <= 40:
            tips.append("燃油经济性较差，建议平稳驾驶减少急加速")
        elif economy <= 65:
            tips.append("燃油经济性一般")
        else:
            tips.append("燃油经济性优秀，继续保持")

        overall = aggressive * 0.4 + (100 - fatigue) * 0.3 + economy * 0.3
        if overall >= 70:
            prefix = "综合驾驶表现优秀。"
        elif overall >= 50:
            prefix = "综合驾驶表现良好。"
        else:
            prefix = "综合驾驶表现有待提升。"

        return prefix + "；".join(tips) + "。"

    def get_result(self) -> Dict[str, Any]:
        aggressive, agg_detail = self.compute_aggressive_index()
        fatigue, fat_detail = self.compute_fatigue_index()
        economy, eco_detail = self.compute_economy_index()
        summary = self.generate_summary(aggressive, fatigue, economy)

        records = self.buffer.get_all()
        latest = records[-1] if records else None
        overall_score = round(
            (100 - aggressive) * 0.35 + (100 - fatigue) * 0.25 + economy * 0.40,
            1
        )

        wm_stats = self.watermark.get_stats()
        time_range = self.buffer.get_time_range()

        return {
            "timestamp": time.time() * 1000,
            "event_time_watermark": wm_stats["watermark"],
            "window_seconds": self.window_seconds,
            "window_event_range": {
                "start": round(time_range[0], 3),
                "end": round(time_range[1], 3)
            },
            "buffer_size": len(self.buffer),
            "watermark_stats": wm_stats,
            "overall_score": overall_score,
            "aggressive_index": aggressive,
            "fatigue_index": fatigue,
            "economy_index": economy,
            "summary": summary,
            "details": {
                "aggressive": agg_detail,
                "fatigue": fat_detail,
                "economy": eco_detail
            },
            "latest_vehicle": {
                "speed": latest["speed"] if latest else 0,
                "rpm": latest["rpm"] if latest else 0,
                "gps": latest["gps"] if latest else {"lat": 0, "lon": 0}
            }
        }
