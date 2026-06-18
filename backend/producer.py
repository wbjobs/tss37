"""
Kafka 数据生产者 - 模拟车辆 CAN 总线数据
周期性发送包含车速、转速、踏板、方向盘、GPS 等信号的 JSON 消息
"""
import json
import time
import random
import math
import sys
import os

try:
    from confluent_kafka import Producer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    print("[警告] confluent_kafka 未安装，将使用标准输出模式")


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "vehicle_can")
SEND_INTERVAL = float(os.getenv("SEND_INTERVAL", "0.1"))


class VehicleSimulator:
    """车辆运动模拟器 - 生成真实的驾驶行为数据"""

    def __init__(self):
        self.timestamp = time.time()
        self.speed = 0.0
        self.rpm = 800.0
        self.throttle = 0.0
        self.brake = 0.0
        self.steering = 0.0
        self.lat = 39.9042
        self.lon = 116.4074
        self.heading = 0.0
        self._mode = "normal"
        self._mode_timer = 0

    def _switch_mode(self):
        """随机切换驾驶模式，模拟不同驾驶风格"""
        self._mode_timer -= 1
        if self._mode_timer <= 0:
            modes = ["normal", "aggressive", "fatigued", "economic"]
            weights = [0.4, 0.2, 0.15, 0.25]
            self._mode = random.choices(modes, weights)[0]
            self._mode_timer = random.randint(50, 200)

    def _update_kinematics(self, dt):
        """根据驾驶模式更新车辆运动学参数"""
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
            gps_dt = dt
            distance = self.speed / 3.6 * gps_dt / 111000
            self.heading += self.steering * 0.0005 * gps_dt
            self.lat += distance * math.cos(math.radians(self.heading))
            self.lon += distance * math.sin(math.radians(self.heading)) / math.cos(math.radians(self.lat))

    def tick(self):
        """生成一帧数据"""
        now = time.time()
        dt = now - self.timestamp
        self.timestamp = now
        self._switch_mode()
        self._update_kinematics(dt)
        return {
            "timestamp": now * 1000,
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
    sim = VehicleSimulator()
    producer = None

    if KAFKA_AVAILABLE:
        try:
            producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
            print(f"[Kafka] 已连接到 {BOOTSTRAP_SERVERS}, 主题: {TOPIC}")
        except Exception as e:
            print(f"[Kafka] 连接失败: {e}, 使用标准输出模式")

    print("[生产者] 开始发送 CAN 总线数据... (Ctrl+C 停止)")
    count = 0
    try:
        while True:
            data = sim.tick()
            payload = json.dumps(data, ensure_ascii=False)

            if producer is not None:
                producer.produce(
                    TOPIC,
                    value=payload.encode("utf-8"),
                    callback=delivery_report
                )
                producer.poll(0)
            else:
                if count % 10 == 0:
                    print(f"[{time.strftime('%H:%M:%S')}] {payload}")

            count += 1
            time.sleep(SEND_INTERVAL)
    except KeyboardInterrupt:
        print("\n[生产者] 已停止")
        if producer is not None:
            producer.flush()


if __name__ == "__main__":
    main()
