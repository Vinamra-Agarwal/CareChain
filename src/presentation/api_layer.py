"""
CareChain Presentation Layer - FastAPI REST APIs, WebSocket & Built-in Dashboard
Enhanced with HTML dashboard for better user interaction and visualization
"""

from fastapi import FastAPI, HTTPException, Depends, WebSocket, WebSocketDisconnect, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import asyncio
import json
import time
from typing import Dict, Any, List, Optional
from pydantic import BaseModel
import jwt
from datetime import datetime, timedelta
import hashlib
import os

# FastAPI app
app = FastAPI(
    title="CareChain Healthcare Blockchain API",
    description="Blockchain-based healthcare data management system with built-in dashboard",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Create templates directory if it doesn't exist
os.makedirs("templates", exist_ok=True)
os.makedirs("static", exist_ok=True)

# Templates and static files
templates = Jinja2Templates(directory="templates")

try:
    app.mount("/static", StaticFiles(directory="static"), name="static")
except:
    pass  # Ignore if static directory doesn't exist

# Security
security = HTTPBearer()
JWT_SECRET = "carechain_secret_key"

# [Previous Pydantic models remain the same]
class UserLogin(BaseModel):
    username: str
    password: str
    user_type: str

class DeviceRegistration(BaseModel):
    device_id: str
    device_type: str
    patient_id: str
    manufacturer: str
    model: str

class PatientData(BaseModel):
    patient_id: str
    encrypted_name: str
    date_of_birth: str
    gender: str
    consent_status: bool

class AccessRequest(BaseModel):
    patient_id: str
    requester_id: str
    data_types: List[str]
    duration_hours: int
    justification: str

# [WebSocketManager and authentication functions remain the same]
class WebSocketManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.user_connections: Dict[str, WebSocket] = {}
    
    async def connect(self, websocket: WebSocket, user_id: str):
        await websocket.accept()
        self.active_connections.append(websocket)
        self.user_connections[user_id] = websocket
    
    def disconnect(self, websocket: WebSocket, user_id: str):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        if user_id in self.user_connections:
            del self.user_connections[user_id]
    
    async def send_personal_message(self, message: dict, user_id: str):
        if user_id in self.user_connections:
            await self.user_connections[user_id].send_text(json.dumps(message))
    
    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_text(json.dumps(message))
            except:
                pass  # Ignore disconnected clients

websocket_manager = WebSocketManager()

def create_jwt_token(user_data: Dict[str, Any]) -> str:
    payload = {
        "user_id": user_data["user_id"],
        "user_type": user_data["user_type"],
        "exp": datetime.utcnow() + timedelta(hours=24),
        "iat": datetime.utcnow()
    }
    return jwt.encode(payload, JWT_SECRET, algorithm="HS256")

def verify_jwt_token(token: str) -> Dict[str, Any]:
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token has expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    return verify_jwt_token(credentials.credentials)

# ========================= DASHBOARD ROUTES =========================

@app.get("/", response_class=HTMLResponse)
async def dashboard_home(request: Request):
    """Main dashboard homepage"""
    return templates.TemplateResponse("dashboard.html", {"request": request})

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    """Login page"""
    return templates.TemplateResponse("login.html", {"request": request})

@app.get("/blockchain", response_class=HTMLResponse)
async def blockchain_dashboard(request: Request):
    """Blockchain monitoring dashboard"""
    return templates.TemplateResponse("blockchain.html", {"request": request})

@app.get("/iot", response_class=HTMLResponse)
async def iot_dashboard(request: Request):
    """IoT devices dashboard"""
    return templates.TemplateResponse("iot.html", {"request": request})

@app.get("/analytics", response_class=HTMLResponse)
async def analytics_dashboard(request: Request):
    """Analytics dashboard"""
    return templates.TemplateResponse("analytics.html", {"request": request})

@app.get("/patients", response_class=HTMLResponse)
async def patients_dashboard(request: Request):
    """Patients management dashboard"""
    return templates.TemplateResponse("patients.html", {"request": request})

# ========================= API ENDPOINTS (Previous endpoints remain) =========================

# Authentication endpoints
@app.post("/api/v1/auth/login")
async def login(user_data: UserLogin):
    if user_data.username and user_data.password:
        user_info = {
            "user_id": f"{user_data.user_type}_{user_data.username}",
            "username": user_data.username,
            "user_type": user_data.user_type
        }
        token = create_jwt_token(user_info)
        return {
            "status": "success",
            "access_token": token,
            "token_type": "bearer",
            "user_info": user_info
        }
    else:
        raise HTTPException(status_code=401, detail="Invalid credentials")

# System status endpoints
@app.get("/api/v1/system/status")
async def get_system_status():
    try:
        from src.iot.iot_layer import iot_manager
        from src.edge.edge_layer import edge_manager
        from src.domain.blockchain_layer import get_blockchain
        from src.application.analytics_layer import analytics_orchestrator
        
        iot_status = iot_manager.get_all_devices_status()
        edge_status = edge_manager.get_all_gateways_status()
        blockchain_status = get_blockchain().get_chain_status()
        analytics_status = analytics_orchestrator.get_analytics_status()
        
        return {
            "system_status": "operational",
            "timestamp": time.time(),
            "layers": {
                "iot": iot_status,
                "edge": edge_status,
                "blockchain": blockchain_status,
                "analytics": analytics_status,
                "presentation": {"active_connections": len(websocket_manager.active_connections)}
            }
        }
    except Exception as e:
        return {
            "system_status": "error",
            "error": str(e),
            "timestamp": time.time()
        }

@app.get("/api/v1/system/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": time.time(),
        "version": "1.0.0"
    }

# Blockchain endpoints
@app.get("/api/v1/blockchain/status")
async def get_blockchain_status():
    try:
        from src.domain.blockchain_layer import get_blockchain
        blockchain = get_blockchain()
        status = blockchain.get_chain_status()
        return status
    except Exception as e:
        return {"error": str(e), "status": "error"}

@app.post("/api/v1/blockchain/mine")
async def mine_block():
    try:
        from src.domain.blockchain_layer import get_blockchain
        blockchain = get_blockchain()
        result = await blockchain.mine_block("admin_miner")
        
        # Broadcast block mined event
        await websocket_manager.broadcast({
            "event": "block_mined",
            "data": result,
            "timestamp": time.time()
        })
        
        return result
    except Exception as e:
        return {"error": str(e), "status": "error"}

# IoT endpoints
@app.get("/api/v1/devices")
async def get_all_devices():
    try:
        from src.iot.iot_layer import iot_manager
        devices_status = iot_manager.get_all_devices_status()
        return devices_status
    except Exception as e:
        return {"error": str(e), "devices": []}

@app.post("/api/v1/devices/register")
async def register_device(device: DeviceRegistration):
    try:
        from src.iot.iot_layer import iot_manager
        result = await iot_manager.register_device(
            device.device_id,
            device.device_type,
            device.patient_id
        )
        
        # Broadcast device registration
        await websocket_manager.broadcast({
            "event": "device_registered",
            "data": result,
            "timestamp": time.time()
        })
        
        return result
    except Exception as e:
        return {"error": str(e), "status": "error"}

# WebSocket endpoint for real-time updates
@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: str):
    await websocket_manager.connect(websocket, user_id)
    try:
        while True:
            data = await websocket.receive_text()
            message = json.loads(data)
            
            # Echo message back
            await websocket_manager.send_personal_message({
                "event": "echo",
                "data": message,
                "timestamp": time.time()
            }, user_id)
            
    except WebSocketDisconnect:
        websocket_manager.disconnect(websocket, user_id)

# Initialize presentation layer
async def initialize_presentation_layer():
    from src.infrastructure.messaging.event_bus import event_bus, LayerType
    
    async def handle_iot_data_event(event):
        await websocket_manager.broadcast({
            "event": "iot_data_received",
            "data": event.payload,
            "timestamp": time.time()
        })

    async def handle_threat_detection_event(event):
        await websocket_manager.broadcast({
            "event": "security_alert",
            "data": event.payload,
            "timestamp": time.time(),
            "priority": "high"
        })

    async def handle_analytics_insight_event(event):
        await websocket_manager.broadcast({
            "event": "healthcare_insight",
            "data": event.payload,
            "timestamp": time.time()
        })
    
    try:
        await event_bus.subscribe_to_layer(LayerType.IOT, handle_iot_data_event)
        await event_bus.subscribe_to_layer(LayerType.EDGE, handle_threat_detection_event)
        await event_bus.subscribe_to_layer(LayerType.ANALYTICS, handle_analytics_insight_event)
    except:
        pass  # Continue if event bus is not available
    
    print("🖥️  Presentation layer initialized with dashboard")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
