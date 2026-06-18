"""
Kafka 数据生产者 v3 - 车队数据模拟器（50 辆车并发）

v3 新增：
  - FLEET_SIZE 环境变量控制车队规模（默认 50）
  - 每辆车有独立的 vehicle_id（veh-001 ~ veh-050）和独立的运动状态
  - 每条消息包含 vehicle_id 字段
  - 车辆按车队轮转发送，每秒约 10 条/车 = 500 条/秒 总吞吐量
  - 继续保留乱序延迟模拟
"""
import json
import time
import random
import math
import os

try:
    from confluent_kafka import Producer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    print("[警告] confluent_kafka 未安装，将使用标准输出模式")


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "vehicle_can")
FLEET_SIZE = int(os.getenv("FLEET_SIZE", "50"))
SEND_INTERVAL = float(os.getenv("SEND_INTERVAL", "0.02"))
OUT_OF_ORDER_MEAN = float(os.getenv("OUT_OF_ORDER_MEAN", "2.0"))


class VehicleSimulator:
    """单车模拟器 - 每辆车独立状态"""

    def __init__(self, vehicle_id: str, out_of_order_mean: float = OUT_OF_ORDER_MEAN,
                 base_lat: float = 39.9042, base_lon: float = 116.4074):
        self.vehicle_id = vehicle_id
        self._wall_time = time.time()
        self._event_time = time.time()
        self.speed = random.uniform(30, 80)
        self.rpm = 800 + self.speed * 30
        self.throttle = random.uniform(10, 40)
        self.brake = 0.0
        self.steering = 0.0
        self.lat = base_lat + random.uniform(-0.05, 0.05)
        self.lon = base_lon + random.uniform(-0.05, 0.05)
        self.heading = random.uniform(0, 360)
        self._mode = random.choice(["normal", "normal", "normal", "aggressive", "fatigued", "economic"])
        self._mode_timer = random.randint(50, 300)
        self._ooo_mean = out_of_order_mean

    def _switch_mode(self):
        self._mode_timer -= 1
        if self._mode_timer <= 0:
            modes = ["normal", "aggressive", "fatigued", "economic"]
            weights = [0.4, 0.2, 0.15, 0.25]
            self._mode = random.choices(modes, weights)[0]
            self._mode_timer = random.randint(50, 300)

    def _update_kinematics(self, dt):
        if self._mode == "aggressive":
            throttle_jitter = random.uniform(-0.15, 0.4)
            brake_jitter = random.uniform(-0.05, 0.3)
            steering_jitter = random.uniform(-60, 60)
            target_accel = random.uniform(-3, 4)
        elif self._mode == "fatigued":
            throttle_jitter = random.uniform(-0.02, 0.05)
            brake_jitter = random.uniform(-0.02, 0.05)
            steering_jitter = random.uniform(-3, 3)
            target_accel = random.uniform(-0.5, 0.5)
        elif self._mode == "economic":
            throttle_jitter = random.uniform(-0.03, 0.1)
            brake_jitter = random.uniform(-0.02, 0.05)
            steering_jitter = random.uniform(-10, 10)
            target_accel = random.uniform(-0.8, 1.2)
        else:
            throttle_jitter = random.uniform(-0.05, 0.2)
            brake_jitter = random.uniform(-0.03, 0.1)
            steering_jitter = random.uniform(-20, 20)
            target_accel = random.uniform(-1.5, 2)

        self.speed = max(0, min(180, self.speed + target_accel * dt * 3.6))
        self.rpm = 800 + self.speed * 30 + random.uniform(-50, 50)
        self.throttle = max(0, min(100, self.throttle + throttle_jitter))
        self.brake = max(0, min(100, self.brake + brake_jitter))
        if self.brake > 5:
            self.throttle = max(0, self.throttle - self.brake * 0.3)
        self.steering = max(-360, min(360, self.steering + steering_jitter))
        self.steering *= 0.95

        if self.speed > 0:
            distance = self.speed / 3.6 * dt / 111000
            self.heading += self.steering * 0.0005 * dt
            self.lat += distance * math.cos(math.radians(self.heading))
            self.lon += distance * math.sin(math.radians(self.heading)) / math.cos(math.radians(self.lat))

    def tick(self):
        now = time.time()
        dt = now - self._wall_time
        self._wall_time = now
        self._event_time += dt

        self._switch_mode()
        self._update_kinematics(dt)

        event_ts = self._event_time
        if self._ooo_mean > 0:
            delay = random.expovariate(1.0 / self._ooo_mean)
            event_ts -= delay

        return {
            "vehicle_id": self.vehicle_id,
            "timestamp": event_ts * 1000,
            "speed": round(self.speed, 2),
            "rpm": round(self.rpm, 1),
            "throttle": round(self.throttle, 2),
            "brake": round(self.brake, 2),
            "steering": round(self.steering, 2),
            "gps": {
                "lat": round(self.lat, 7),
                "lon": round(self.lon, 7)
            }
        }


def delivery_report(err, msg):
    if err is not None:
        print(f"[Kafka] 消息发送失败: {err}")


def main():
    vehicles = [
        VehicleSimulator(f"veh-{i+1:03d}", out_of_order_mean=OUT_OF_ORDER_MEAN)
        for i in range(FLEET_SIZE)
    ]
    producer = None

    if KAFKA_AVAILABLE:
        try:
            producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
            print(f"[Kafka] 已连接到 {BOOTSTRAP_SERVERS}, 主题: {TOPIC}")
        except Exception as e:
            print(f"[Kafka] 连接失败: {e}, 使用标准输出模式")

    print(f"[生产者 v3] 车队规模: {FLEET_SIZE} 辆车")
    print(f"[生产者 v3] 发送间隔: {SEND_INTERVAL}s, 乱序均值: {OUT_OF_ORDER_MEAN}s")
    print(f"[生产者 v3] 预估吞吐量: {1/SEND_INTERVAL:.0f} msg/s, 每车 {(1/SEND_INTERVAL)/FLEET_SIZE:.1f} msg/s")
    print(f"[生产者 v3] 开始发送数据... (Ctrl+C 停止)")

    count = 0
    vehicle_idx = 0
    try:
        while True:
            veh = vehicles[vehicle_idx % FLEET_SIZE]
            vehicle_idx += 1
            data = veh.tick()
            payload = json.dumps(data, ensure_ascii=False)

            if producer is not None:
                producer.produce(
                    TOPIC,
                    key=data["vehicle_id"].encode("utf-8"),
                    value=payload.encode("utf-8"),
                    callback=delivery_report
                )
                producer.poll(0)
            else:
                if count % 500 == 0:
                    print(f"[{time.strftime('%H:%M:%S')}] veh={data['vehicle_id']} speed={data['speed']}")

            count += 1
            time.sleep(SEND_INTERVAL)
    except KeyboardInterrupt:
        print(f"\n[生产者] 已停止，共发送 {count} 条消息")
        if producer is not None:
            producer.flush()


if __name__ == "__main__":
    main()
