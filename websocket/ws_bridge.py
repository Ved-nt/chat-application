#!/usr/bin/env python3
"""
ws_bridge.py — WebSocket bridge that keeps persistent writer TCP sockets
for writer sessions so the server can maintain the writer lock (wrt).
"""

import asyncio
import websockets
import socket
import json
from datetime import datetime

TCP_HOST = "127.0.0.1"
TCP_PORT = 8080
WS_HOST = "0.0.0.0"
WS_PORT = 8765

# track connected websockets and writer -> tcp socket mapping
connected = set()
# map websocket object id to a persistent TCP socket for writer sessions
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
            # shutdown writing so server knows request finished
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

            # ---------- READER: one-shot ----------
            if role == "reader":
                # use one-shot TCP to fetch DB contents
                loop = asyncio.get_running_loop()
                resp = await loop.run_in_executor(None, tcp_one_shot, "reader", None, None)
                await ws.send(json.dumps({"status":"ok","role":"reader","data":resp}))
                continue

            # ---------- WRITER ----------
            # For writer we maintain persistent TCP socket stored in writer_tcp_map keyed by id(ws)
            wid = id(ws)

            # START: create persistent connection and send "writer" then "start"
            if control == "start":
                # If already has a socket, reply accordingly
                if wid in writer_tcp_map:
                    await ws.send(json.dumps({"status":"error","message":"writer session already active"}))
                    continue
                try:
                    # establish TCP connection
                    s = socket.create_connection((TCP_HOST, TCP_PORT), timeout=5)
                except Exception as e:
                    await ws.send(json.dumps({"status":"error","message":f"TCP connect error: {e}"}))
                    continue

                # send role then start, but DON'T close the socket — keep it open
                resp = await asyncio.get_running_loop().run_in_executor(None, tcp_send_and_recv_sock, s, b"writer", True)
                # The above may read a reply if server responded; but usually we just continue to send start
                # Now send start and wait for server ack (server will sem_wait and reply when lock acquired)
                resp2 = await asyncio.get_running_loop().run_in_executor(None, tcp_send_and_recv_sock, s, b"start", True)
                # Check response
                if resp2.startswith("OK"):
                    writer_tcp_map[wid] = s
                    await ws.send(json.dumps({"status":"ok","role":"writer","reply":resp2}))
                else:
                    # failed to get lock or DB issue — close socket
                    try:
                        s.close()
                    except:
                        pass
                    await ws.send(json.dumps({"status":"error","message":resp2}))
                continue

            # STOP: find persistent socket, send stop and close
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

            # MESSAGE: must use existing persistent socket (writer must have started)
            if message:
                s = writer_tcp_map.get(wid)
                if not s:
                    await ws.send(json.dumps({"status":"error","message":"start writer session first"}))
                    continue
                # send the message on existing socket and get response (ack)
                resp = await asyncio.get_running_loop().run_in_executor(None, tcp_send_and_recv_sock, s, message.encode("utf-8"), True)
                # Only broadcast if server replied "OK"
                if resp.startswith("OK"):
                    # broadcast payload
                    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                    bobj = {"message": message, "timestamp": timestamp}
                    await broadcast(bobj)
                    await ws.send(json.dumps({"status":"ok","role":"writer","reply":resp}))
                else:
                    await ws.send(json.dumps({"status":"error","message":resp}))
                continue

            # If we reach here, missing control/message
            await ws.send(json.dumps({"status":"error","message":"no control or message provided"}))

    except websockets.exceptions.ConnectionClosed:
        print("[WS] Client disconnected")
    finally:
        # cleanup writer socket if any
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
