"""
CareChain IoT Device Layer - Healthcare Device Authentication & Data Collection
Single file implementation for IoT devices connecting to blockchain
"""

import asyncio
import hashlib
import json
import time
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from datetime import datetime
import random

@dataclass
class SensorReading:
    """Healthcare sensor data reading"""
    device_id: str
    sensor_type: str
    value: float
    unit: str
    timestamp: float
    quality_score: float = 1.0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "device_id": self.device_id,
            "sensor_type": self.sensor_type,
            "value": self.value,
            "unit": self.unit,
            "timestamp": self.timestamp,
            "quality_score": self.quality_score
        }

class IoTDevice:
    """Healthcare IoT Device with blockchain integration"""
    
    def __init__(self, device_id: str, device_type: str, patient_id: str):
        self.device_id = device_id
        self.device_type = device_type  # heart_monitor, glucose_meter, etc.
        self.patient_id = patient_id
        self.puf_signature = self._generate_puf_signature()
        self.device_token = None
        self.is_authenticated = False
        self.last_heartbeat = time.time()
        self.sensor_readings: List[SensorReading] = []
        
    def _generate_puf_signature(self) -> str:
        """Generate Physical Unclonable Function signature"""
        # Simplified PUF simulation - in reality this would be hardware-based
        device_unique_data = f"{self.device_id}_{self.device_type}_{time.time()}"
        return hashlib.sha256(device_unique_data.encode()).hexdigest()
    
    async def authenticate(self) -> Dict[str, Any]:
        """Authenticate device using PUF challenge-response"""
        try:
            # Simulate PUF challenge-response
            challenge = f"challenge_{int(time.time())}"
            response = hashlib.sha256(f"{self.puf_signature}_{challenge}".encode()).hexdigest()
            
            # Generate device token
            self.device_token = hashlib.sha256(f"{self.device_id}_{response}".encode()).hexdigest()[:32]
            self.is_authenticated = True
            
            return {
                "status": "authenticated",
                "device_id": self.device_id,
                "device_token": self.device_token,
                "timestamp": time.time()
            }
            
        except Exception as e:
            return {
                "status": "authentication_failed",
                "error": str(e)
            }
    
    async def collect_sensor_data(self) -> SensorReading:
        """Collect data from healthcare sensors"""
        # Simulate sensor data based on device type
        sensor_data = self._simulate_sensor_reading()
        reading = SensorReading(
            device_id=self.device_id,
            sensor_type=sensor_data["type"],
            value=sensor_data["value"],
            unit=sensor_data["unit"],
            timestamp=time.time(),
            quality_score=random.uniform(0.8, 1.0)
        )
        
        self.sensor_readings.append(reading)
        return reading
    
    def _simulate_sensor_reading(self) -> Dict[str, Any]:
        """Simulate healthcare sensor readings"""
        if self.device_type == "heart_monitor":
            return {
                "type": "heart_rate",
                "value": random.uniform(60, 100),
                "unit": "bpm"
            }
        elif self.device_type == "glucose_meter":
            return {
                "type": "blood_glucose",
                "value": random.uniform(70, 140),
                "unit": "mg/dL"
            }
        elif self.device_type == "blood_pressure":
            return {
                "type": "blood_pressure",
                "value": random.uniform(110, 140),
                "unit": "mmHg"
            }
        elif self.device_type == "temperature_sensor":
            return {
                "type": "body_temperature",
                "value": random.uniform(97.0, 99.5),
                "unit": "°F"
            }
        else:
            return {
                "type": "generic",
                "value": random.uniform(0, 100),
                "unit": "units"
            }
    
    async def send_heartbeat(self) -> Dict[str, Any]:
        """Send device heartbeat"""
        self.last_heartbeat = time.time()
        return {
            "device_id": self.device_id,
            "status": "online",
            "timestamp": self.last_heartbeat,
            "battery_level": random.uniform(20, 100)  # Simulated battery
        }
    
    async def prepare_blockchain_transaction(self, sensor_reading: SensorReading) -> Dict[str, Any]:
        """Prepare sensor data for blockchain submission"""
        if not self.is_authenticated:
            raise Exception("Device not authenticated")
        
        # Encrypt sensor data (simplified)
        encrypted_data = self._encrypt_sensor_data(sensor_reading)
        
        return {
            "patient_id": self.patient_id,
            "device_id": self.device_id,
            "data_type": sensor_reading.sensor_type,
            "encrypted_data": encrypted_data,
            "timestamp": sensor_reading.timestamp,
            "sender": self.device_id
        }
    
    def _encrypt_sensor_data(self, reading: SensorReading) -> str:
        """Encrypt sensor data for blockchain storage"""
        # Simplified encryption - in production use proper encryption
        data_json = json.dumps(reading.to_dict())
        encrypted = hashlib.sha256(f"{self.device_token}_{data_json}".encode()).hexdigest()
        return encrypted

class IoTDeviceManager:
    """Manages multiple IoT devices and blockchain integration"""
    
    def __init__(self):
        self.devices: Dict[str, IoTDevice] = {}
        self.is_running = False
        
    async def register_device(self, device_id: str, device_type: str, patient_id: str) -> Dict[str, Any]:
        """Register new IoT device"""
        if device_id in self.devices:
            return {"status": "error", "message": "Device already registered"}
        
        device = IoTDevice(device_id, device_type, patient_id)
        auth_result = await device.authenticate()
        
        if auth_result["status"] == "authenticated":
            self.devices[device_id] = device
            return {
                "status": "registered",
                "device_id": device_id,
                "device_token": device.device_token
            }
        else:
            return {"status": "registration_failed", "error": auth_result.get("error")}
    
    async def start_data_collection(self):
        """Start continuous data collection from all devices"""
        self.is_running = True
        
        async def collect_from_device(device: IoTDevice):
            while self.is_running:
                try:
                    # Collect sensor data
                    reading = await device.collect_sensor_data()
                    
                    # Prepare for blockchain
                    blockchain_data = await device.prepare_blockchain_transaction(reading)
                    
                    # Send to edge layer (simulated)
                    await self._send_to_edge_layer(blockchain_data)
                    
                    # Send heartbeat
                    await device.send_heartbeat()
                    
                    # Wait before next reading
                    await asyncio.sleep(random.uniform(5, 15))  # 5-15 seconds
                    
                except Exception as e:
                    print(f"Error collecting data from {device.device_id}: {e}")
                    await asyncio.sleep(30)  # Wait longer on error
        
        # Start collection tasks for all devices
        tasks = [collect_from_device(device) for device in self.devices.values()]
        await asyncio.gather(*tasks)
    
    async def _send_to_edge_layer(self, data: Dict[str, Any]):
        """Send data to edge layer (simulated)"""
        # In real implementation, this would send to edge gateway
        print(f"IoT → Edge: {data['device_id']} sent {data['data_type']} data")
        
        # Simulate sending to event bus
        from src.infrastructure.messaging.event_bus import layer_communicator
        await layer_communicator.publish_iot_data(
            device_id=data['device_id'],
            sensor_data=data
        )
    
    def get_device_status(self, device_id: str) -> Dict[str, Any]:
        """Get device status"""
        device = self.devices.get(device_id)
        if not device:
            return {"status": "not_found"}
        
        return {
            "device_id": device.device_id,
            "device_type": device.device_type,
            "patient_id": device.patient_id,
            "is_authenticated": device.is_authenticated,
            "last_heartbeat": device.last_heartbeat,
            "total_readings": len(device.sensor_readings),
            "status": "online" if time.time() - device.last_heartbeat < 60 else "offline"
        }
    
    def get_all_devices_status(self) -> Dict[str, Any]:
        """Get status of all devices"""
        return {
            "total_devices": len(self.devices),
            "devices": [self.get_device_status(device_id) for device_id in self.devices.keys()],
            "is_collecting": self.is_running
        }
    
    async def stop_data_collection(self):
        """Stop data collection"""
        self.is_running = False

# Global IoT device manager
iot_manager = IoTDeviceManager()

# Helper functions
async def simulate_patient_devices(patient_id: str, num_devices: int = 3):
    """Simulate multiple devices for a patient"""
    device_types = ["heart_monitor", "glucose_meter", "blood_pressure", "temperature_sensor"]
    
    for i in range(num_devices):
        device_id = f"device_{patient_id}_{i+1}"
        device_type = device_types[i % len(device_types)]
        
        result = await iot_manager.register_device(device_id, device_type, patient_id)
        print(f"Device registration: {result}")

async def start_iot_simulation():
    """Start IoT layer simulation"""
    # Register some test devices
    await simulate_patient_devices("patient_001", 2)
    await simulate_patient_devices("patient_002", 2)
    
    # Start data collection
    print("Starting IoT data collection...")
    await iot_manager.start_data_collection()
