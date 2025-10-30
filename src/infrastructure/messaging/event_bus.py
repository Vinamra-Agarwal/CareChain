"""
CareChain Event Bus - Inter-Layer Communication System
Redis-based pub/sub messaging for real-time healthcare data flow
"""

import asyncio
import json
import redis.asyncio as redis
from typing import Dict, Any, Callable, List, Optional
from datetime import datetime
from dataclasses import dataclass, asdict
import logging
import os
from enum import Enum

logger = logging.getLogger(__name__)

class LayerType(Enum):
    """Five architectural layers"""
    IOT = "iot"
    EDGE = "edge" 
    BLOCKCHAIN = "blockchain"
    ANALYTICS = "analytics"
    PRESENTATION = "presentation"

@dataclass
class LayerEvent:
    """Event structure for inter-layer communication"""
    event_id: str
    source_layer: LayerType
    target_layer: LayerType
    event_type: str
    payload: Dict[str, Any]
    timestamp: datetime
    priority: int = 1  # 1=high, 2=medium, 3=low
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "source_layer": self.source_layer.value,
            "target_layer": self.target_layer.value,
            "event_type": self.event_type,
            "payload": self.payload,
            "timestamp": self.timestamp.isoformat(),
            "priority": self.priority
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'LayerEvent':
        return cls(
            event_id=data["event_id"],
            source_layer=LayerType(data["source_layer"]),
            target_layer=LayerType(data["target_layer"]),
            event_type=data["event_type"],
            payload=data["payload"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            priority=data.get("priority", 1)
        )

class EventBus:
    """Redis-based event bus for CareChain layer communication"""
    
    def __init__(self, redis_url: str = None):
        self.redis_url = redis_url or os.getenv("REDIS_URL", "redis://localhost:6379")
        self.redis_client: Optional[redis.Redis] = None
        self.subscribers: Dict[str, List[Callable]] = {}
        self.is_connected = False
        
    async def connect(self):
        """Initialize Redis connection"""
        try:
            self.redis_client = redis.from_url(
                self.redis_url,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_keepalive=True,
                health_check_interval=30
            )
            # Test connection
            await self.redis_client.ping()
            self.is_connected = True
            logger.info("EventBus connected to Redis")
            print("📡 EventBus connected to Redis")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            print(f"❌ Failed to connect to Redis: {e}")
            # For MVP, continue without Redis if not available
            self.is_connected = False
    
    async def disconnect(self):
        """Close Redis connection"""
        if self.redis_client:
            await self.redis_client.close()
            self.is_connected = False
            logger.info("EventBus disconnected from Redis")
    
    async def publish_event(self, event: LayerEvent) -> bool:
        """Publish event to specific layer channel"""
        # For MVP, if Redis is not available, just log the event
        if not self.is_connected:
            print(f"📤 Event: {event.event_type} from {event.source_layer.value} to {event.target_layer.value}")
            return True
            
        try:
            channel = f"carechain:{event.target_layer.value}"
            message = json.dumps(event.to_dict())
            
            # Publish to specific layer
            await self.redis_client.publish(channel, message)
            
            # Also publish to global event stream for monitoring
            await self.redis_client.publish("carechain:events", message)
            
            logger.info(f"Published {event.event_type} from {event.source_layer.value} to {event.target_layer.value}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to publish event: {e}")
            print(f"❌ Failed to publish event: {e}")
            return False
    
    async def subscribe_to_layer(self, layer: LayerType, callback: Callable):
        """Subscribe to events for a specific layer"""
        channel = f"carechain:{layer.value}"
        
        if channel not in self.subscribers:
            self.subscribers[channel] = []
        
        self.subscribers[channel].append(callback)
        
        # For MVP, if Redis is not available, just register the callback
        if not self.is_connected:
            print(f"📝 Subscribed to layer {layer.value} (offline mode)")
            return
        
        # Start listening if this is the first subscriber for this channel
        if len(self.subscribers[channel]) == 1:
            asyncio.create_task(self._listen_to_channel(channel))
        
        logger.info(f"Subscribed to layer {layer.value}")
    
    async def _listen_to_channel(self, channel: str):
        """Listen to Redis channel and execute callbacks"""
        if not self.is_connected:
            return
            
        pubsub = self.redis_client.pubsub()
        await pubsub.subscribe(channel)
        
        try:
            async for message in pubsub.listen():
                if message["type"] == "message":
                    try:
                        event_data = json.loads(message["data"])
                        event = LayerEvent.from_dict(event_data)
                        
                        # Execute all callbacks for this channel
                        for callback in self.subscribers.get(channel, []):
                            try:
                                if asyncio.iscoroutinefunction(callback):
                                    await callback(event)
                                else:
                                    callback(event)
                            except Exception as e:
                                logger.error(f"Callback error: {e}")
                                
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")
        except Exception as e:
            logger.error(f"Error listening to channel {channel}: {e}")
        finally:
            await pubsub.unsubscribe(channel)

class LayerCommunicator:
    """High-level interface for CareChain layer communication"""
    
    def __init__(self, event_bus: EventBus):
        self.event_bus = event_bus
    
    # IoT Layer Communications
    async def publish_iot_data(self, device_id: str, sensor_data: Dict[str, Any]):
        """IoT device publishes sensor data to Edge layer"""
        event = LayerEvent(
            event_id=f"iot_data_{device_id}_{datetime.now().timestamp()}",
            source_layer=LayerType.IOT,
            target_layer=LayerType.EDGE,
            event_type="sensor_data",
            payload={
                "device_id": device_id,
                "sensor_data": sensor_data,
                "timestamp": datetime.now().isoformat()
            },
            timestamp=datetime.now(),
            priority=1
        )
        return await self.event_bus.publish_event(event)
    
    # Edge Layer Communications
    async def publish_edge_processed_data(self, gateway_id: str, processed_data: Dict[str, Any]):
        """Edge gateway publishes processed data to Blockchain layer"""
        event = LayerEvent(
            event_id=f"edge_processed_{gateway_id}_{datetime.now().timestamp()}",
            source_layer=LayerType.EDGE,
            target_layer=LayerType.BLOCKCHAIN,
            event_type="processed_data",
            payload={
                "gateway_id": gateway_id,
                "processed_data": processed_data
            },
            timestamp=datetime.now(),
            priority=1
        )
        return await self.event_bus.publish_event(event)
    
    async def publish_threat_detection(self, gateway_id: str, threat_info: Dict[str, Any]):
        """Edge gateway publishes threat detection to all layers"""
        for target_layer in [LayerType.BLOCKCHAIN, LayerType.ANALYTICS, LayerType.PRESENTATION]:
            event = LayerEvent(
                event_id=f"threat_{gateway_id}_{datetime.now().timestamp()}",
                source_layer=LayerType.EDGE,
                target_layer=target_layer,
                event_type="security_threat",
                payload={
                    "gateway_id": gateway_id,
                    "threat_info": threat_info
                },
                timestamp=datetime.now(),
                priority=1
            )
            await self.event_bus.publish_event(event)
    
    # Analytics Layer Communications
    async def publish_analytics_insight(self, analysis_id: str, insights: Dict[str, Any]):
        """Analytics publishes healthcare insights"""
        event = LayerEvent(
            event_id=f"insight_{analysis_id}_{datetime.now().timestamp()}",
            source_layer=LayerType.ANALYTICS,
            target_layer=LayerType.PRESENTATION,
            event_type="healthcare_insight",
            payload={
                "analysis_id": analysis_id,
                "insights": insights
            },
            timestamp=datetime.now(),
            priority=2
        )
        return await self.event_bus.publish_event(event)

# Global event bus and communicator instances
event_bus = EventBus()
layer_communicator = LayerCommunicator(event_bus)

# Health check function
async def health_check() -> bool:
    """Check if event bus is healthy"""
    try:
        if not event_bus.is_connected:
            await event_bus.connect()
        if event_bus.redis_client:
            await event_bus.redis_client.ping()
            return True
        return False
    except Exception:
        return False
