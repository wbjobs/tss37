"""
驾驶员行为画像计算引擎
实现三个维度的指数计算：激进指数、疲劳指数、经济性指数
采用滑动窗口机制（默认5分钟），每10秒输出一次综合结果
"""
import math
import time
from collections import deque
from typing import List, Dict, Any, Tuple


WINDOW_SECONDS = 300
OUTPUT_INTERVAL = 10

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


class BehaviorAnalyzer:
    """驾驶员行为分析器 - 维护滑动窗口并计算三个指数"""

    def __init__(self, window_seconds: int = WINDOW_SECONDS):
        self.window_seconds = window_seconds
        self.raw_records: deque = deque()
        self.acceleration_events: deque = deque()
        self.deceleration_events: deque = deque()
        self.steer_events: deque = deque()
        self.last_output_time = 0.0

    def add_record(self, record: Dict[str, Any]):
        """添加一条新的 CAN 数据记录到滑动窗口"""
        now_ms = record["timestamp"]
        now_s = now_ms / 1000.0

        self.raw_records.append(record)

        cutoff = now_s - self.window_seconds
        while self.raw_records and self.raw_records[0]["timestamp"] / 1000.0 < cutoff:
            self.raw_records.popleft()
        while self.acceleration_events and self.acceleration_events[0] < cutoff:
            self.acceleration_events.popleft()
        while self.deceleration_events and self.deceleration_events[0] < cutoff:
            self.deceleration_events.popleft()
        while self.steer_events and self.steer_events[0] < cutoff:
            self.steer_events.popleft()

        if len(self.raw_records) >= 2:
            prev = self.raw_records[-2]
            curr = self.raw_records[-1]
            self._detect_events(prev, curr)

    def _detect_events(self, prev: Dict, curr: Dict):
        """检测急加速、急减速、急转弯等驾驶事件"""
        dt = (curr["timestamp"] - prev["timestamp"]) / 1000.0
        if dt <= 0:
            return

        dv = (curr["speed"] - prev["speed"]) / 3.6
        accel = dv / dt
        curr_s = curr["timestamp"] / 1000.0

        if accel >= ACCELERATION_THRESHOLD:
            self.acceleration_events.append(curr_s)
        if accel <= DECELERATION_THRESHOLD:
            self.deceleration_events.append(curr_s)

        ds = abs(curr["steering"] - prev["steering"])
        steer_rate = ds / dt
        if steer_rate >= STEER_RATE_THRESHOLD:
            self.steer_events.append(curr_s)

    def should_output(self) -> bool:
        """判断是否到达输出时间间隔"""
        now = time.time()
        if now - self.last_output_time >= OUTPUT_INTERVAL:
            self.last_output_time = now
            return True
        return False

    def compute_aggressive_index(self) -> Tuple[float, Dict]:
        """计算激进指数 (0-100, 分数越高越激进)"""
        if len(self.raw_records) < 5:
            return 30.0, {"accel_count": 0, "decel_count": 0, "steer_count": 0}

        window_duration = self.window_seconds / 60.0
        accel_freq = len(self.acceleration_events) / window_duration
        decel_freq = len(self.deceleration_events) / window_duration
        steer_freq = len(self.steer_events) / window_duration

        accel_score = min(100.0, accel_freq * 15.0)
        decel_score = min(100.0, decel_freq * 15.0)
        steer_score = min(100.0, steer_freq * 10.0)

        index = accel_score * 0.35 + decel_score * 0.35 + steer_score * 0.30

        return round(min(100.0, index), 1), {
            "accel_count": len(self.acceleration_events),
            "decel_count": len(self.deceleration_events),
            "steer_count": len(self.steer_events)
        }

    def compute_fatigue_index(self) -> Tuple[float, Dict]:
        """计算疲劳指数 (0-100, 分数越高越疲劳)"""
        records = list(self.raw_records)
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
        """计算三点的 GPS 轨迹曲率（Menger 曲率）"""
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
        """计算经济性指数 (0-100, 分数越高越经济)"""
        records = list(self.raw_records)
        if len(records) < 10:
            return 60.0, {"avg_fuel": 0.0, "coast_ratio": 0.0, "idle_ratio": 0.0}

        total_fuel = 0.0
        total_speed = 0.0
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
            total_speed += speed

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
        """估算瞬时油耗 (L/100km 的瞬时模拟值)"""
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
        """根据三个指数生成简短的行为摘要文本"""
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
        """获取当前滑动窗口的完整计算结果"""
        aggressive, agg_detail = self.compute_aggressive_index()
        fatigue, fat_detail = self.compute_fatigue_index()
        economy, eco_detail = self.compute_economy_index()
        summary = self.generate_summary(aggressive, fatigue, economy)

        latest = self.raw_records[-1] if self.raw_records else None
        overall_score = round(
            (100 - aggressive) * 0.35 + (100 - fatigue) * 0.25 + economy * 0.40,
            1
        )

        return {
            "timestamp": time.time() * 1000,
            "window_seconds": self.window_seconds,
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
