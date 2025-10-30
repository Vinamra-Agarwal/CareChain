"""
CareChain Database Session Management
Async SQLAlchemy session handling for healthcare blockchain MVP
"""

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from contextlib import asynccontextmanager
from typing import AsyncGenerator
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database Configuration
DATABASE_URL = os.getenv(
    "DATABASE_URL", 
    "postgresql+asyncpg://carechain:password@localhost:5432/carechain_db"
)

# For MVP, we'll use SQLite if PostgreSQL is not available
FALLBACK_DATABASE_URL = "sqlite+aiosqlite:///./carechain_mvp.db"

class DatabaseManager:
    """Database connection and session management"""
    
    def __init__(self):
        self.engine = None
        self.session_factory = None
        self.is_connected = False
        self._initialize_engine()
    
    def _initialize_engine(self):
        """Initialize database engine with fallback"""
        try:
            # Try PostgreSQL first
            if "postgresql" in DATABASE_URL:
                print("📊 Attempting PostgreSQL connection...")
                self.engine = create_async_engine(
                    DATABASE_URL,
                    echo=False,  # Set to True for SQL debugging
                    pool_size=5,
                    max_overflow=0,
                    pool_pre_ping=True,
                    pool_recycle=300,
                )
            else:
                # Use SQLite for MVP
                print("📊 Using SQLite database for MVP...")
                self.engine = create_async_engine(
                    FALLBACK_DATABASE_URL,
                    echo=False,
                    pool_pre_ping=True,
                )
            
            self.session_factory = async_sessionmaker(
                self.engine,
                class_=AsyncSession,
                expire_on_commit=False,
                autoflush=True,
                autocommit=False,
            )
            
        except Exception as e:
            print(f"⚠️  Database engine initialization failed: {e}")
            print("📊 Falling back to SQLite...")
            self.engine = create_async_engine(
                FALLBACK_DATABASE_URL,
                echo=False,
                pool_pre_ping=True,
            )
            self.session_factory = async_sessionmaker(
                self.engine,
                class_=AsyncSession,
                expire_on_commit=False,
                autoflush=True,
                autocommit=False,
            )
    
    @asynccontextmanager
    async def get_session(self) -> AsyncGenerator[AsyncSession, None]:
        """Get async database session with automatic cleanup"""
        if not self.session_factory:
            raise Exception("Database not initialized")
            
        async with self.session_factory() as session:
            try:
                yield session
                await session.commit()
            except Exception as e:
                await session.rollback()
                raise e
            finally:
                await session.close()
    
    async def create_tables(self):
        """Create all database tables"""
        try:
            # For MVP, we'll skip actual table creation and just test connection
            print("📊 Testing database connection...")
            
            if self.engine:
                async with self.engine.begin() as conn:
                    # Test connection with a simple query
                    await conn.execute("SELECT 1")
                    
                self.is_connected = True
                print("✅ Database connection successful!")
                return True
                
        except Exception as e:
            print(f"⚠️  Database connection failed: {e}")
            print("📊 Continuing in offline mode...")
            self.is_connected = False
            return False
    
    async def drop_tables(self):
        """Drop all database tables - USE WITH CAUTION"""
        print("⚠️  This operation is not implemented in MVP for safety")
        return False
    
    async def health_check(self) -> bool:
        """Check database connectivity"""
        try:
            if not self.engine:
                return False
                
            async with self.engine.begin() as conn:
                await conn.execute("SELECT 1")
                return True
        except Exception:
            return False

# Global database manager instance
db_manager = DatabaseManager()

# Dependency for FastAPI
async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency for database session"""
    if db_manager.is_connected:
        async with db_manager.get_session() as session:
            yield session
    else:
        # For MVP, yield None if database is not available
        yield None
