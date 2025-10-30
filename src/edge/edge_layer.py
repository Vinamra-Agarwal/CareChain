"""
CareChain Edge Computing Layer - Gateway Processing & Local Consensus
Single file implementation for edge gateways processing IoT data
"""

import asyncio
import hashlib
import json
import time
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from datetime import datetime
import statistics

@dataclass
class ProcessedData:
    """Processed healthcare data at edge"""
    gateway_id: str
    device_id: str
    processed_value: float
    aggregation_type: str
    confidence_score: float
    timestamp: float
    raw_data_hash: str

class EdgeGateway:
    """Healthcare Edge Computing Gateway"""
    
    def __init__(self, gateway_id: str, location: str):
        self.gateway_id = gateway_id
        self.location = location
        self.connected_devices: Dict[str, Dict[str, Any]] = {}
        self.data_buffer: List[Dict[str, Any]] = []
        self.processing_rules: Dict[str, Any] = {}
        self.threat_detection_enabled = True
        self.local_cache: Dict[str, Any] = {}
        self.status = "online"
        
        # Edge blockchain consensus
        self.local_consensus_nodes: List[str] = []
        self.consensus_threshold = 2
        
    async def register_device(self, device_data: Dict[str, Any]) -> Dict[str, Any]:
        """Register IoT device with edge gateway"""
        device_id = device_data["device_id"]
        
        self.connected_devices[device_id] = {
            "device_id": device_id,
            "device_type": device_data.get("device_type", "unknown"),
            "patient_id": device_data.get("patient_id"),
            "registered_at": time.time(),
            "last_data_received": None,
            "data_count": 0,
            "status": "active"
        }
        
        return {
            "status": "registered",
            "gateway_id": self.gateway_id,
            "device_id": device_id
        }
    
    async def receive_iot_data(self, iot_data: Dict[str, Any]) -> Dict[str, Any]:
        """Receive and process IoT sensor data"""
        device_id = iot_data["device_id"]
        
        # Update device status
        if device_id in self.connected_devices:
            self.connected_devices[device_id]["last_data_received"] = time.time()
            self.connected_devices[device_id]["data_count"] += 1
        
        # Add to data buffer for processing
        self.data_buffer.append({
            **iot_data,
            "received_at": time.time(),
            "gateway_id": self.gateway_id
        })
        
        # Process data
        processed_result = await self._process_sensor_data(iot_data)
        
        # Threat detection
        threat_result = await self._detect_threats(iot_data)
        
        # Cache locally
        self._cache_data(device_id, iot_data)
        
        return {
            "status": "processed",
            "gateway_id": self.gateway_id,
            "device_id": device_id,
            "processed_data": processed_result,
            "threat_detected": threat_result["threat_detected"]
        }
    
    async def _process_sensor_data(self, iot_data: Dict[str, Any]) -> ProcessedData:
        """Process sensor data with edge intelligence"""
        device_id = iot_data["device_id"]
        sensor_data = iot_data["sensor_data"]
        
        # Get historical data for this device
        historical_data = self._get_cached_data(device_id, limit=10)
        
        # Perform edge analytics
        if historical_data:
            values = [d.get("value", 0) for d in historical_data]
            processed_value = statistics.mean(values)  # Moving average
            confidence_score = min(1.0, len(values) / 10)  # More data = higher confidence
        else:
            processed_value = sensor_data.get("value", 0)
            confidence_score = 0.5
        
        # Anomaly detection
        if historical_data and len(historical_data) > 3:
            mean_val = statistics.mean([d.get("value", 0) for d in historical_data])
            std_val = statistics.stdev([d.get("value", 0) for d in historical_data])
            current_val = sensor_data.get("value", 0)
            
            if abs(current_val - mean_val) > 2 * std_val:
                confidence_score *= 0.7  # Lower confidence for anomalies
        
        raw_data_hash = hashlib.sha256(json.dumps(sensor_data, sort_keys=True).encode()).hexdigest()
        
        return ProcessedData(
            gateway_id=self.gateway_id,
            device_id=device_id,
            processed_value=processed_value,
            aggregation_type="moving_average",
            confidence_score=confidence_score,
            timestamp=time.time(),
            raw_data_hash=raw_data_hash
        )
    
    async def _detect_threats(self, iot_data: Dict[str, Any]) -> Dict[str, Any]:
        """Real-time threat detection"""
        if not self.threat_detection_enabled:
            return {"threat_detected": False}
        
        device_id = iot_data["device_id"]
        sensor_data = iot_data["sensor_data"]
        
        threats = []
        
        # Check for suspicious data patterns
        if self._is_suspicious_pattern(sensor_data):
            threats.append("suspicious_data_pattern")
        
        # Check for device tampering
        if self._detect_device_tampering(device_id, iot_data):
            threats.append("device_tampering")
        
        # Check for replay attacks
        if self._detect_replay_attack(device_id, iot_data):
            threats.append("replay_attack")
        
        if threats:
            await self._handle_threat_response(device_id, threats)
        
        return {
            "threat_detected": len(threats) > 0,
            "threats": threats,
            "timestamp": time.time()
        }
    
    def _is_suspicious_pattern(self, sensor_data: Dict[str, Any]) -> bool:
        """Check for suspicious data patterns"""
        # Simplified pattern detection
        value = sensor_data.get("value", 0)
        
        # Check for impossible values
        data_type = sensor_data.get("type", "")
        if data_type == "heart_rate" and (value < 30 or value > 200):
            return True
        elif data_type == "blood_glucose" and (value < 20 or value > 600):
            return True
        elif data_type == "body_temperature" and (value < 95 or value > 110):
            return True
        
        return False
    
    def _detect_device_tampering(self, device_id: str, iot_data: Dict[str, Any]) -> bool:
        """Detect potential device tampering"""
        # Check device signature consistency
        if device_id in self.connected_devices:
            # Simplified tampering detection
            current_time = time.time()
            last_received = self.connected_devices[device_id]["last_data_received"]
            
            if last_received and current_time - last_received < 1:  # Too frequent
                return True
        
        return False
    
    def _detect_replay_attack(self, device_id: str, iot_data: Dict[str, Any]) -> bool:
        """Detect replay attacks"""
        # Check for duplicate timestamps or data
        timestamp = iot_data.get("timestamp", 0)
        cached_data = self._get_cached_data(device_id, limit=5)
        
        for cached in cached_data:
            if abs(cached.get("timestamp", 0) - timestamp) < 0.1:  # Very similar timestamp
                return True
        
        return False
    
    async def _handle_threat_response(self, device_id: str, threats: List[str]):
        """Handle detected threats"""
        # Log threat
        print(f"THREAT DETECTED - Gateway {self.gateway_id}, Device {device_id}: {threats}")
        
        # Notify blockchain layer
        from src.infrastructure.messaging.event_bus import layer_communicator
        await layer_communicator.publish_threat_detection(
            gateway_id=self.gateway_id,
            threat_info={
                "device_id": device_id,
                "threats": threats,
                "timestamp": time.time(),
                "severity": "high" if "device_tampering" in threats else "medium"
            }
        )
        
        # Update device status
        if device_id in self.connected_devices:
            self.connected_devices[device_id]["status"] = "threat_detected"
    
    def _cache_data(self, device_id: str, data: Dict[str, Any]):
        """Cache data locally at edge"""
        if device_id not in self.local_cache:
            self.local_cache[device_id] = []
        
        self.local_cache[device_id].append({
            **data,
            "cached_at": time.time()
        })
        
        # Keep only recent data (last 100 entries)
        if len(self.local_cache[device_id]) > 100:
            self.local_cache[device_id] = self.local_cache[device_id][-100:]
    
    def _get_cached_data(self, device_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get cached data for device"""
        if device_id in self.local_cache:
            return self.local_cache[device_id][-limit:]
        return []
    
    async def perform_local_consensus(self, data_batch: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Perform local consensus with nearby edge nodes"""
        consensus_result = {
            "gateway_id": self.gateway_id,
            "batch_size": len(data_batch),
            "consensus_nodes": self.local_consensus_nodes,
            "timestamp": time.time(),
            "status": "approved"
        }
        
        # Simplified local consensus (in reality would communicate with other nodes)
        if len(self.local_consensus_nodes) >= self.consensus_threshold:
            consensus_result["votes"] = len(self.local_consensus_nodes)
            consensus_result["approved"] = True
        else:
            consensus_result["approved"] = False
            consensus_result["reason"] = "insufficient_nodes"
        
        return consensus_result
    
    async def send_to_blockchain_layer(self, processed_data: ProcessedData) -> Dict[str, Any]:
        """Send processed data to blockchain layer"""
        blockchain_payload = {
            "gateway_id": self.gateway_id,
            "processed_data": {
                "device_id": processed_data.device_id,
                "processed_value": processed_data.processed_value,
                "confidence_score": processed_data.confidence_score,
                "timestamp": processed_data.timestamp,
                "raw_data_hash": processed_data.raw_data_hash
            }
        }
        
        # Send via event bus
        from src.infrastructure.messaging.event_bus import layer_communicator
        await layer_communicator.publish_edge_processed_data(
            gateway_id=self.gateway_id,
            processed_data=blockchain_payload
        )
        
        return {"status": "sent_to_blockchain", "timestamp": time.time()}
    
    def get_gateway_status(self) -> Dict[str, Any]:
        """Get gateway status"""
        return {
            "gateway_id": self.gateway_id,
            "location": self.location,
            "status": self.status,
            "connected_devices": len(self.connected_devices),
            "data_buffer_size": len(self.data_buffer),
            "threat_detection_enabled": self.threat_detection_enabled,
            "local_consensus_nodes": len(self.local_consensus_nodes)
        }

class EdgeManager:
    """Manages multiple edge gateways"""
    
    def __init__(self):
        self.gateways: Dict[str, EdgeGateway] = {}
        self.is_running = False
    
    async def create_gateway(self, gateway_id: str, location: str) -> Dict[str, Any]:
        """Create new edge gateway"""
        if gateway_id in self.gateways:
            return {"status": "exists", "message": "Gateway already exists"}
        
        gateway = EdgeGateway(gateway_id, location)
        self.gateways[gateway_id] = gateway
        
        return {
            "status": "created",
            "gateway_id": gateway_id,
            "location": location
        }
    
    async def process_iot_event(self, iot_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process IoT data event from event bus"""
        # Route to appropriate gateway (simplified - use first available)
        if not self.gateways:
            return {"status": "no_gateways"}
        
        gateway = list(self.gateways.values())[0]  # Use first gateway for MVP
        result = await gateway.receive_iot_data(iot_data)
        
        # Send processed data to blockchain layer
        if "processed_data" in result:
            await gateway.send_to_blockchain_layer(result["processed_data"])
        
        return result
    
    def get_all_gateways_status(self) -> Dict[str, Any]:
        """Get status of all gateways"""
        return {
            "total_gateways": len(self.gateways),
            "gateways": [gateway.get_gateway_status() for gateway in self.gateways.values()],
            "is_running": self.is_running
        }

# Global edge manager
edge_manager = EdgeManager()

# Initialize with default gateway for MVP
async def initialize_edge_layer():
    """Initialize edge layer with default gateway"""
    await edge_manager.create_gateway("gateway_001", "Hospital_Main")
    await edge_manager.create_gateway("gateway_002", "Hospital_ICU")
    print("Edge layer initialized with default gateways")
