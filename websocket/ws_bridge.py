

import asyncio
import websockets
import socket
import json
from datetime import datetime

TCP_HOST = "127.0.0.1"
TCP_PORT = 8080
WS_HOST = "0.0.0.0"
WS_PORT = 8765

connected = set()
writer_tcp_map = {}

def tcp_send_and_recv_sock(s: socket.socket, data: bytes, expect_reply=True, shutdown_write=False, timeout=5):
    """Send bytes on existing connected socket and read reply (blocking)."""
    try:
        s.sendall(data)
        if shutdown_write:
            try:
                s.shutdown(socket.SHUT_WR)
            except OSError:
                pass
        if not expect_reply:
            return ""
        s.settimeout(timeout)
        resp = b""
        while True:
            try:
                chunk = s.recv(4096)
            except socket.timeout:
                break
            if not chunk:
                break
            resp += chunk
        return resp.decode("utf-8", errors="replace")
    except Exception as e:
        return f"TCP error: {e}"

def tcp_one_shot(role: str, control: str = None, message: str = None, timeout=5) -> str:
    """Create a short-lived TCP connection, send role and control/message, read reply, close."""
    try:
        with socket.create_connection((TCP_HOST, TCP_PORT), timeout=timeout) as s:
            s.sendall(role.encode("utf-8"))
            if role == "writer":
                if control:
                    s.sendall(control.encode("utf-8"))
                elif message:
                    s.sendall(message.encode("utf-8"))
            try:
                s.shutdown(socket.SHUT_WR)
            except OSError:
                pass
            resp = b""
            while True:
                chunk = s.recv(4096)
                if not chunk:
                    break
                resp += chunk
            return resp.decode("utf-8", errors="replace")
    except Exception as e:
        return f"TCP error: {e}"

async def broadcast(payload: dict):
    if not connected:
        return
    data = json.dumps({"type": "broadcast", "payload": payload})
    await asyncio.gather(*(ws.send(data) for ws in connected if ws.open))

async def handle_websocket(ws, path):
    print("[WS] Client connected")
    connected.add(ws)
    try:
        async for raw in ws:
            try:
                data = json.loads(raw)
            except Exception:
                await ws.send(json.dumps({"status":"error","message":"invalid json"}))
                continue

            role = data.get("role")
            message = data.get("message", "")
            control = data.get("control", None)

            if role not in ("reader","writer"):
                await ws.send(json.dumps({"status":"error","message":"role must be 'reader' or 'writer'"}))
                continue
            if role == "reader":
                loop = asyncio.get_running_loop()
                resp = await loop.run_in_executor(None, tcp_one_shot, "reader", None, None)
                await ws.send(json.dumps({"status":"ok","role":"reader","data":resp}))
                continue

            wid = id(ws)
            if control == "start":
                if wid in writer_tcp_map:
                    await ws.send(json.dumps({"status":"error","message":"writer session already active"}))
                    continue
                try:
                    s = socket.create_connection((TCP_HOST, TCP_PORT), timeout=5)
                except Exception as e:
                    await ws.send(json.dumps({"status":"error","message":f"TCP connect error: {e}"}))
                    continue

                resp = await asyncio.get_running_loop().run_in_executor(None, tcp_send_and_recv_sock, s, b"writer", True)

                resp2 = await asyncio.get_running_loop().run_in_executor(None, tcp_send_and_recv_sock, s, b"start", True)

                if resp2.startswith("OK"):
                    writer_tcp_map[wid] = s
                    await ws.send(json.dumps({"status":"ok","role":"writer","reply":resp2}))
                else:
                    try:
                        s.close()
                    except:
                        pass
                    await ws.send(json.dumps({"status":"error","message":resp2}))
                continue

            if control == "stop":
                s = writer_tcp_map.get(wid)
                if not s:
                    await ws.send(json.dumps({"status":"error","message":"no active writer session"}))
                    continue
                resp = await asyncio.get_running_loop().run_in_executor(None, tcp_send_and_recv_sock, s, b"stop", True)
                try:
                    s.close()
                except:
                    pass
                writer_tcp_map.pop(wid, None)
                await ws.send(json.dumps({"status":"ok","role":"writer","reply":resp}))
                continue

            if message:
                s = writer_tcp_map.get(wid)
                if not s:
                    await ws.send(json.dumps({"status":"error","message":"start writer session first"}))
                    continue
                resp = await asyncio.get_running_loop().run_in_executor(None, tcp_send_and_recv_sock, s, message.encode("utf-8"), True)
                if resp.startswith("OK"):
                    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                    bobj = {"message": message, "timestamp": timestamp}
                    await broadcast(bobj)
                    await ws.send(json.dumps({"status":"ok","role":"writer","reply":resp}))
                else:
                    await ws.send(json.dumps({"status":"error","message":resp}))
                continue

            await ws.send(json.dumps({"status":"error","message":"no control or message provided"}))

    except websockets.exceptions.ConnectionClosed:
        print("[WS] Client disconnected")
    finally:
        wid = id(ws)
        s = writer_tcp_map.pop(wid, None)
        if s:
            try:
                s.close()
            except:
                pass
        connected.remove(ws)

async def main():
    print(f"[WS] Serving on ws://{WS_HOST}:{WS_PORT}")
    async with websockets.serve(handle_websocket, WS_HOST, WS_PORT):
        await asyncio.Future()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("WS server stopped")

