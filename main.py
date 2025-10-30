"""
CareChain MVP Standalone Runner with Built-in Dashboard
Enhanced with comprehensive web interface and visualization
"""

import asyncio
import uvicorn
import os
import sys

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

async def startup_mvp():
    """Initialize CareChain MVP with dashboard in standalone mode"""
    print("🚀 Starting CareChain Healthcare Blockchain MVP with Dashboard...")
    print("📋 Running in standalone/local mode")
    
    try:
        # Set environment variables for local run
        os.environ['DATABASE_URL'] = 'sqlite+aiosqlite:///./carechain_mvp.db'
        os.environ['REDIS_URL'] = 'redis://localhost:6379'
        
        # Import after setting environment
        from src.presentation.api_layer import app, initialize_presentation_layer
        from src.infrastructure.messaging.event_bus import event_bus
        from src.domain.blockchain_layer import initialize_blockchain
        from src.iot.iot_layer import iot_manager, simulate_patient_devices
        from src.edge.edge_layer import initialize_edge_layer
        from src.application.analytics_layer import initialize_analytics_layer
        
        # Initialize infrastructure
        print("📡 Connecting to event bus...")
        await event_bus.connect()
        
        # Initialize blockchain core
        print("⛓️  Initializing blockchain...")
        blockchain = initialize_blockchain([
            "validator_hospital_001",
            "validator_hospital_002", 
            "validator_research_center"
        ])
        
        # Initialize all layers
        print("🔧 Initializing IoT layer...")
        await simulate_patient_devices("patient_001", 3)
        await simulate_patient_devices("patient_002", 2)
        
        print("🌐 Initializing edge layer...")
        await initialize_edge_layer()
        
        print("📈 Initializing analytics layer...")
        await initialize_analytics_layer()
        
        print("🖥️  Initializing presentation layer with dashboard...")
        await initialize_presentation_layer()
        
        print("✅ CareChain MVP successfully initialized!")
        print(f"🏥 Blockchain validators: {blockchain.validators}")
        print(f"📱 IoT devices registered: {len(iot_manager.devices)}")
        
        return app
        
    except Exception as e:
        print(f"❌ Startup failed: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    # Run startup
    print("🔄 Starting CareChain MVP with Dashboard...")
    app = asyncio.run(startup_mvp())
    
    if app:
        print("\n🌟 Starting FastAPI server with built-in dashboard...")
        print("🌐 Main Dashboard: http://localhost:8000")
        print("⛓️  Blockchain Dashboard: http://localhost:8000/blockchain")
        print("📱 IoT Dashboard: http://localhost:8000/iot")
        print("📊 Analytics Dashboard: http://localhost:8000/analytics")
        print("📋 API Documentation: http://localhost:8000/docs")
        print("🔄 WebSocket endpoint: ws://localhost:8000/ws/{user_id}")
        print("💡 Press Ctrl+C to stop the server")
        
        uvicorn.run(
            app,
            host="0.0.0.0",
            port=8000,
            reload=False,
            log_level="info"
        )
    else:
        print("❌ Failed to start CareChain MVP")
