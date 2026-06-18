"""
主服务端 v2 - 集成 Kafka 消费者、事件时间水印分析引擎、WebSocket 服务端

核心改进（对应 v2 水印机制）：
  1. add_record() 现在返回记录分类（on_time/late/drop），用于监控水印效果
  2. should_output() 基于事件时间水印推进触发，而非处理时间
  3. 模拟器生成带随机延迟的乱序数据，以验证水印机制的鲁棒性

支持两种数据输入模式：
  1. Kafka 模式 - 从 Kafka 消费 CAN 总线数据
  2. 回退模式 - 直接调用内部模拟器生成数据（无需 Kafka 环境）
同时启动 HTTP 静态文件服务和 WebSocket 推送服务
"""
import asyncio
import json
import os
import sys
import time
import threading
import http.server
import socketserver
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from behavior_analyzer import BehaviorAnalyzer

try:
    from confluent_kafka import Consumer, KafkaException
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

try:
    import websockets
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "vehicle_can")
WS_HOST = os.getenv("WS_HOST", "0.0.0.0")
WS_PORT = int(os.getenv("WS_PORT", "8765"))
HTTP_PORT = int(os.getenv("HTTP_PORT", "8080"))
FRONTEND_DIR = Path(__file__).parent.parent / "frontend"


ALLOWED_LATENESS = float(os.getenv("ALLOWED_LATENESS", "5.0"))

analyzer = BehaviorAnalyzer(allowed_lateness=ALLOWED_LATENESS)
connected_clients: set = set()
latest_result: dict = None
trajectory_buffer: list = []
TRAJECTORY_MAX_POINTS = 500
watermark_log_interval = 0
watermark_log_counter = 0


class ProducerSimulator:
    """
    内置数据模拟器 v2 - 带乱序模拟

    生成数据时给事件时间戳添加随机"网络延迟偏移"，
    模拟 Kafka 高峰期消息乱序到达的场景。
    延迟偏移量服从指数分布，均值由 OUT_OF_ORDER_MEAN 控制。
    """

    def __init__(self, out_of_order_mean: float = 2.0):
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
        self._event_time = time.time()
        self._out_of_order_mean = out_of_order_mean
        print(f"[模拟器] 乱序延迟均值: {out_of_order_mean}s")

    def _switch_mode(self):
        self._mode_timer -= 1
        if self._mode_timer <= 0:
            modes = ["normal", "aggressive", "fatigued", "economic"]
            weights = [0.4, 0.2, 0.15, 0.25]
            self._mode = __import__("random").choices(modes, weights)[0]
            self._mode_timer = __import__("random").randint(50, 200)

    def tick(self, dt):
        import random
        import math
        self._switch_mode()
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

        now = time.time()
        self._event_time += dt
        event_ts = self._event_time

        delay_offset = random.expovariate(1.0 / self._out_of_order_mean) if self._out_of_order_mean > 0 else 0
        event_ts -= delay_offset

        return {
            "timestamp": event_ts * 1000,
            "speed": round(self.speed, 2),
            "rpm": round(self.rpm, 1),
            "throttle": round(self.throttle, 2),
            "brake": round(self.brake, 2),
            "steering": round(self.steering, 2),
            "gps": {"lat": round(self.lat, 7), "lon": round(self.lon, 7)}
        }


def kafka_consumer_thread():
    """Kafka 消费者线程 - 从 Kafka 读取数据并送入分析器"""
    if not KAFKA_AVAILABLE:
        print("[数据] Kafka 库未安装，使用内置模拟器")
        simulator_fallback_thread()
        return

    try:
        consumer = Consumer({
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": "behavior_analyzer",
            "auto.offset.reset": "latest"
        })
        consumer.subscribe([TOPIC])
        print(f"[Kafka] 已连接 {BOOTSTRAP_SERVERS}, 订阅主题: {TOPIC}")

        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[Kafka] 错误: {msg.error()}")
                continue
            try:
                record = json.loads(msg.value().decode("utf-8"))
                process_record(record)
            except Exception as e:
                print(f"[Kafka] 解析消息失败: {e}")
    except KafkaException as e:
        print(f"[Kafka] 连接异常: {e}，切换到内置模拟器")
        simulator_fallback_thread()


def simulator_fallback_thread():
    """内置模拟器线程 - Kafka 不可用时作为数据源"""
    sim = ProducerSimulator()
    last_tick = time.time()
    print("[模拟器] 已启动，开始生成模拟驾驶数据")
    while True:
        now = time.time()
        dt = now - last_tick
        last_tick = now
        record = sim.tick(dt)
        process_record(record)
        time.sleep(0.1)


def process_record(record: dict):
    """处理单条 CAN 数据记录（v2: 含水印分类反馈）"""
    global latest_result, watermark_log_counter

    result = analyzer.add_record(record)

    if result["status"] == "drop":
        if watermark_log_counter % 100 == 0:
            print(f"[Watermark] 丢弃迟到数据: event_time={result['event_time']}, watermark={result['watermark']}")
        watermark_log_counter += 1
        return

    gps = record.get("gps", {})
    if gps:
        trajectory_buffer.append({
            "timestamp": record["timestamp"],
            "lat": gps["lat"],
            "lon": gps["lon"],
            "aggressive": 50
        })
        while len(trajectory_buffer) > TRAJECTORY_MAX_POINTS:
            trajectory_buffer.pop(0)

    if analyzer.should_output():
        latest_result = analyzer.get_result()
        asyncio.run_coroutine_threadsafe(broadcast_result(), asyncio_loop)


async def broadcast_result():
    """广播最新计算结果给所有已连接的 WebSocket 客户端"""
    if not latest_result or not connected_clients:
        return
    payload = json.dumps({
        "type": "metrics",
        "data": latest_result,
        "trajectory": list(trajectory_buffer[-100:])
    }, ensure_ascii=False)
    disconnected = set()
    for ws in connected_clients:
        try:
            await ws.send(payload)
        except Exception:
            disconnected.add(ws)
    for ws in disconnected:
        connected_clients.discard(ws)


async def handle_websocket(websocket):
    """处理 WebSocket 客户端连接"""
    print(f"[WebSocket] 客户端已连接: {websocket.remote_address}")
    connected_clients.add(websocket)
    try:
        if latest_result:
            initial = json.dumps({
                "type": "metrics",
                "data": latest_result,
                "trajectory": list(trajectory_buffer)
            }, ensure_ascii=False)
            await websocket.send(initial)
        async for _ in websocket:
            pass
    finally:
        connected_clients.discard(websocket)
        print(f"[WebSocket] 客户端已断开: {websocket.remote_address}")


def start_http_server():
    """启动 HTTP 静态文件服务，提供前端仪表盘"""
    class Handler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=str(FRONTEND_DIR), **kwargs)

        def log_message(self, format, *args):
            pass

    with socketserver.TCPServer((WS_HOST, HTTP_PORT), Handler) as httpd:
        print(f"[HTTP] 前端仪表盘地址: http://localhost:{HTTP_PORT}")
        httpd.serve_forever()


async def ws_main():
    """WebSocket 服务主协程"""
    print(f"[WebSocket] 监听端口: {WS_PORT}")
    async with websockets.serve(handle_websocket, WS_HOST, WS_PORT):
        await asyncio.Future()


if __name__ == "__main__":
    asyncio_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(asyncio_loop)

    http_thread = threading.Thread(target=start_http_server, daemon=True)
    http_thread.start()

    kafka_thread = threading.Thread(target=kafka_consumer_thread, daemon=True)
    kafka_thread.start()

    print("=" * 60)
    print("  车联网驾驶员行为画像实时分析系统 v2")
    print("  [事件时间语义 + Watermark 水印机制]")
    print("=" * 60)
    print(f"  前端仪表盘: http://localhost:{HTTP_PORT}")
    print(f"  WebSocket : ws://localhost:{WS_PORT}")
    print(f"  水印宽容度: {ALLOWED_LATENESS}s (迟到数据容忍上限)")
    if KAFKA_AVAILABLE:
        print(f"  Kafka     : {BOOTSTRAP_SERVERS} (主题: {TOPIC})")
    else:
        print(f"  Kafka     : 未连接，使用内置数据模拟器(含乱序)")
    print("=" * 60)

    try:
        asyncio_loop.run_until_complete(ws_main())
    except KeyboardInterrupt:
        print("\n[服务] 已停止")
