"""
主服务端 v3 - 车队管理系统

v3 架构升级：
  1. FleetStateManager: 按 vehicle_id 分区管理 50 辆车的独立窗口状态
  2. AlertEngine: 阈值检测 + 去重冷却 + Kafka/WebSocket 双通道告警
  3. WebSocket 推送:
       - fleet_summary: 车队聚合统计（定时推）
       - vehicle_update: 某辆车画像更新（触发时推）
       - alert: 异常告警（触发时推）
  4. 模拟器: 50 辆车并发 + 带 vehicle_id + 乱序延迟

支持前端两种模式：
  - 车队总览: 所有车辆列表 + 告警 + 聚合统计
  - 单车下钻: 某辆车详细画像 + 轨迹
"""
import asyncio
import json
import os
import sys
import time
import random
import math
import threading
import http.server
import socketserver
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from fleet_manager import FleetStateManager

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
FLEET_SIZE = int(os.getenv("FLEET_SIZE", "50"))
ALLOWED_LATENESS = float(os.getenv("ALLOWED_LATENESS", "5.0"))
WS_HOST = os.getenv("WS_HOST", "0.0.0.0")
WS_PORT = int(os.getenv("WS_PORT", "8765"))
HTTP_PORT = int(os.getenv("HTTP_PORT", "8080"))
FRONTEND_DIR = Path(__file__).parent.parent / "frontend"
FLEET_PUSH_INTERVAL = float(os.getenv("FLEET_PUSH_INTERVAL", "2.0"))


fleet = FleetStateManager(allowed_lateness=ALLOWED_LATENESS)
connected_clients: set = set()


class FleetSimulator:
    """50 辆车并发模拟器（无需 Kafka 环境时使用）"""

    def __init__(self, fleet_size: int = FLEET_SIZE, out_of_order_mean: float = 2.0):
        self.fleet_size = fleet_size
        self.vehicles = []
        for i in range(fleet_size):
            self.vehicles.append({
                "vehicle_id": f"veh-{i+1:03d}",
                "_wall_time": time.time(),
                "_event_time": time.time(),
                "speed": random.uniform(30, 80),
                "rpm": 0,
                "throttle": random.uniform(10, 40),
                "brake": 0.0,
                "steering": 0.0,
                "lat": 39.9042 + random.uniform(-0.05, 0.05),
                "lon": 116.4074 + random.uniform(-0.05, 0.05),
                "heading": random.uniform(0, 360),
                "_mode": random.choice(["normal", "normal", "normal", "aggressive", "fatigued", "economic"]),
                "_mode_timer": random.randint(50, 300),
                "_ooo_mean": out_of_order_mean,
            })
        print(f"[模拟器] 已初始化 {fleet_size} 辆车，乱序均值 {out_of_order_mean}s")

    def tick(self, veh: dict, dt: float):
        veh["_wall_time"] = time.time()
        veh["_event_time"] += dt

        veh["_mode_timer"] -= 1
        if veh["_mode_timer"] <= 0:
            modes = ["normal", "aggressive", "fatigued", "economic"]
            weights = [0.4, 0.2, 0.15, 0.25]
            veh["_mode"] = random.choices(modes, weights)[0]
            veh["_mode_timer"] = random.randint(50, 300)

        if veh["_mode"] == "aggressive":
            tj = random.uniform(-0.15, 0.4)
            bj = random.uniform(-0.05, 0.3)
            sj = random.uniform(-60, 60)
            ta = random.uniform(-3, 4)
        elif veh["_mode"] == "fatigued":
            tj = random.uniform(-0.02, 0.05)
            bj = random.uniform(-0.02, 0.05)
            sj = random.uniform(-3, 3)
            ta = random.uniform(-0.5, 0.5)
        elif veh["_mode"] == "economic":
            tj = random.uniform(-0.03, 0.1)
            bj = random.uniform(-0.02, 0.05)
            sj = random.uniform(-10, 10)
            ta = random.uniform(-0.8, 1.2)
        else:
            tj = random.uniform(-0.05, 0.2)
            bj = random.uniform(-0.03, 0.1)
            sj = random.uniform(-20, 20)
            ta = random.uniform(-1.5, 2)

        veh["speed"] = max(0, min(180, veh["speed"] + ta * dt * 3.6))
        veh["rpm"] = 800 + veh["speed"] * 30 + random.uniform(-50, 50)
        veh["throttle"] = max(0, min(100, veh["throttle"] + tj))
        veh["brake"] = max(0, min(100, veh["brake"] + bj))
        if veh["brake"] > 5:
            veh["throttle"] = max(0, veh["throttle"] - veh["brake"] * 0.3)
        veh["steering"] = max(-360, min(360, veh["steering"] + sj))
        veh["steering"] *= 0.95

        if veh["speed"] > 0:
            distance = veh["speed"] / 3.6 * dt / 111000
            veh["heading"] += veh["steering"] * 0.0005 * dt
            veh["lat"] += distance * math.cos(math.radians(veh["heading"]))
            veh["lon"] += distance * math.sin(math.radians(veh["heading"])) / math.cos(math.radians(veh["lat"]))

        event_ts = veh["_event_time"]
        if veh["_ooo_mean"] > 0:
            delay = random.expovariate(1.0 / veh["_ooo_mean"])
            event_ts -= delay

        return {
            "vehicle_id": veh["vehicle_id"],
            "timestamp": event_ts * 1000,
            "speed": round(veh["speed"], 2),
            "rpm": round(veh["rpm"], 1),
            "throttle": round(veh["throttle"], 2),
            "brake": round(veh["brake"], 2),
            "steering": round(veh["steering"], 2),
            "gps": {"lat": round(veh["lat"], 7), "lon": round(veh["lon"], 7)}
        }


def kafka_consumer_thread():
    if not KAFKA_AVAILABLE:
        print("[数据] Kafka 库未安装，使用内置车队模拟器")
        simulator_thread()
        return
    try:
        consumer = Consumer({
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": "fleet_analyzer",
            "auto.offset.reset": "latest"
        })
        consumer.subscribe([TOPIC])
        print(f"[Kafka] 已连接 {BOOTSTRAP_SERVERS}, 订阅: {TOPIC}")
        while True:
            msg = consumer.poll(timeout=0.1)
            if msg is None:
                continue
            if msg.error():
                print(f"[Kafka] 错误: {msg.error()}")
                continue
            try:
                record = json.loads(msg.value().decode("utf-8"))
                process_record(record)
            except Exception as e:
                print(f"[Kafka] 解析失败: {e}")
    except KafkaException as e:
        print(f"[Kafka] 异常: {e}，切换模拟器")
        simulator_thread()


def simulator_thread():
    sim = FleetSimulator(fleet_size=FLEET_SIZE)
    last_tick = time.time()
    idx = 0
    print("[模拟器] 车队数据流启动")
    while True:
        now = time.time()
        dt = now - last_tick
        last_tick = now

        for _ in range(5):
            veh = sim.vehicles[idx % len(sim.vehicles)]
            idx += 1
            record = sim.tick(veh, dt / 5)
            process_record(record)

        time.sleep(0.02)


def process_record(record: dict):
    result = fleet.add_record(record)

    if result["new_alerts"]:
        for alert in result["new_alerts"]:
            print(f"[告警] {alert['severity'].upper()} {alert['alert_type']} vehicle={alert['vehicle_id']} "
                  f"value={alert['index_value']} threshold={alert['threshold']}")
            asyncio.run_coroutine_threadsafe(broadcast({
                "type": "alert",
                "data": alert
            }), asyncio_loop)

    if result["output_triggered"] and result["vehicle_result"]:
        vdata = result["vehicle_result"]
        trajectory = fleet.get_vehicle_trajectory(result["vehicle_id"])
        asyncio.run_coroutine_threadsafe(broadcast({
            "type": "vehicle_update",
            "vehicle_id": result["vehicle_id"],
            "vehicle_name": fleet.get_vehicle_name(result["vehicle_id"]),
            "data": vdata,
            "trajectory": trajectory
        }), asyncio_loop)


def fleet_push_loop():
    """定时向所有客户端推送车队聚合统计"""
    while True:
        time.sleep(FLEET_PUSH_INTERVAL)
        summary = fleet.get_fleet_summary()
        vehicles = fleet.get_all_vehicle_status()
        alerts = fleet.get_recent_alerts(limit=30)
        asyncio.run_coroutine_threadsafe(broadcast({
            "type": "fleet_summary",
            "data": summary,
            "vehicles": vehicles,
            "recent_alerts": alerts
        }), asyncio_loop)


async def broadcast(message: dict):
    if not connected_clients:
        return
    payload = json.dumps(message, ensure_ascii=False)
    disconnected = set()
    for ws in connected_clients:
        try:
            await ws.send(payload)
        except Exception:
            disconnected.add(ws)
    for ws in disconnected:
        connected_clients.discard(ws)


async def handle_websocket(websocket):
    print(f"[WebSocket] 客户端已连接: {websocket.remote_address}")
    connected_clients.add(websocket)
    try:
        summary = fleet.get_fleet_summary()
        vehicles = fleet.get_all_vehicle_status()
        alerts = fleet.get_recent_alerts(limit=30)
        initial = json.dumps({
            "type": "fleet_summary",
            "data": summary,
            "vehicles": vehicles,
            "recent_alerts": alerts
        }, ensure_ascii=False)
        await websocket.send(initial)

        async for raw_msg in websocket:
            try:
                msg = json.loads(raw_msg)
                if msg.get("type") == "drill_down":
                    vid = msg.get("vehicle_id")
                    vdata = fleet.get_vehicle_result(vid)
                    traj = fleet.get_vehicle_trajectory(vid)
                    if vdata:
                        resp = json.dumps({
                            "type": "vehicle_update",
                            "vehicle_id": vid,
                            "vehicle_name": fleet.get_vehicle_name(vid),
                            "data": vdata,
                            "trajectory": traj
                        }, ensure_ascii=False)
                        await websocket.send(resp)
            except Exception:
                pass
    finally:
        connected_clients.discard(websocket)
        print(f"[WebSocket] 客户端已断开: {websocket.remote_address}")


def start_http_server():
    class Handler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=str(FRONTEND_DIR), **kwargs)
        def log_message(self, format, *args):
            pass
    with socketserver.TCPServer((WS_HOST, HTTP_PORT), Handler) as httpd:
        print(f"[HTTP] 前端仪表盘: http://localhost:{HTTP_PORT}")
        httpd.serve_forever()


async def ws_main():
    print(f"[WebSocket] 监听: ws://{WS_HOST}:{WS_PORT}")
    async with websockets.serve(handle_websocket, WS_HOST, WS_PORT):
        await asyncio.Future()


if __name__ == "__main__":
    asyncio_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(asyncio_loop)

    threading.Thread(target=start_http_server, daemon=True).start()
    threading.Thread(target=kafka_consumer_thread, daemon=True).start()
    threading.Thread(target=fleet_push_loop, daemon=True).start()

    print("=" * 70)
    print("  车联网车队驾驶员行为画像系统 v3")
    print("  [多分区状态管理 + 事件时间水印 + 异常告警]")
    print("=" * 70)
    print(f"  前端仪表盘     : http://localhost:{HTTP_PORT}")
    print(f"  WebSocket      : ws://localhost:{WS_PORT}")
    print(f"  车队规模       : {FLEET_SIZE} 辆车")
    print(f"  水印宽容度     : {ALLOWED_LATENESS}s")
    if KAFKA_AVAILABLE:
        print(f"  Kafka          : {BOOTSTRAP_SERVERS} (topic: {TOPIC})")
    else:
        print(f"  Kafka          : 未连接，使用内置车队模拟器")
    print("=" * 70)

    try:
        asyncio_loop.run_until_complete(ws_main())
    except KeyboardInterrupt:
        print("\n[服务] 已停止")
