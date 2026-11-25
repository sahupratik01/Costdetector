from fastapi import WebSocket


class ProgressHub:
    def __init__(self) -> None:
        self.rooms: dict[str, set[WebSocket]] = {}
        self.buffers: dict[str, list[dict]] = {}

    async def publish(self, analysis_id: str, payload: dict) -> None:
        self.buffers.setdefault(analysis_id, []).append(payload)
        dead: list[WebSocket] = []
        for socket in self.rooms.get(analysis_id, set()):
            try:
                await socket.send_json(payload)
            except Exception:
                dead.append(socket)
        for socket in dead:
            self.rooms.get(analysis_id, set()).discard(socket)

    async def connect(self, analysis_id: str, socket: WebSocket) -> None:
        self.rooms.setdefault(analysis_id, set()).add(socket)
        for payload in self.buffers.get(analysis_id, []):
            await socket.send_json(payload)

    def disconnect(self, analysis_id: str, socket: WebSocket) -> None:
        self.rooms.get(analysis_id, set()).discard(socket)


hub = ProgressHub()
