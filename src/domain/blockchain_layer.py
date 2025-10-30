"""
CareChain Blockchain Layer - Core Consensus and Transaction Management
Single file implementation focusing on healthcare blockchain functionality
"""

import hashlib
import json
import time
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from datetime import datetime
import asyncio
from enum import Enum

class TransactionType(Enum):
    PATIENT_DATA = "patient_data"
    DEVICE_REGISTRATION = "device_registration"
    ACCESS_GRANT = "access_grant"
    MEDICAL_RECORD = "medical_record"
    CONSENT_UPDATE = "consent_update"

class ConsensusStatus(Enum):
    PENDING = "pending"
    VALIDATED = "validated"
    REJECTED = "rejected"
    COMMITTED = "committed"

@dataclass
class Transaction:
    """Healthcare blockchain transaction"""
    transaction_id: str
    transaction_type: TransactionType
    patient_id: str
    data_hash: str
    payload: Dict[str, Any]
    timestamp: float
    sender: str
    signature: str
    gas_fee: float = 0.001
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "transaction_id": self.transaction_id,
            "transaction_type": self.transaction_type.value,
            "patient_id": self.patient_id,
            "data_hash": self.data_hash,
            "payload": self.payload,
            "timestamp": self.timestamp,
            "sender": self.sender,
            "signature": self.signature,
            "gas_fee": self.gas_fee
        }
    
    def calculate_hash(self) -> str:
        """Calculate transaction hash"""
        tx_string = json.dumps(self.to_dict(), sort_keys=True)
        return hashlib.sha256(tx_string.encode()).hexdigest()

@dataclass
class Block:
    """Healthcare blockchain block"""
    block_number: int
    previous_hash: str
    timestamp: float
    transactions: List[Transaction]
    merkle_root: str
    validator: str
    signature: str
    nonce: int = 0
    
    def calculate_hash(self) -> str:
        """Calculate block hash"""
        block_data = {
            "block_number": self.block_number,
            "previous_hash": self.previous_hash,
            "timestamp": self.timestamp,
            "merkle_root": self.merkle_root,
            "validator": self.validator,
            "nonce": self.nonce,
            "transaction_count": len(self.transactions)
        }
        block_string = json.dumps(block_data, sort_keys=True)
        return hashlib.sha256(block_string.encode()).hexdigest()
    
    def calculate_merkle_root(self) -> str:
        """Calculate Merkle tree root of transactions"""
        if not self.transactions:
            return hashlib.sha256(b"").hexdigest()
        
        tx_hashes = [tx.calculate_hash() for tx in self.transactions]
        
        while len(tx_hashes) > 1:
            new_hashes = []
            for i in range(0, len(tx_hashes), 2):
                if i + 1 < len(tx_hashes):
                    combined = tx_hashes[i] + tx_hashes[i + 1]
                else:
                    combined = tx_hashes[i] + tx_hashes[i]
                new_hashes.append(hashlib.sha256(combined.encode()).hexdigest())
            tx_hashes = new_hashes
        
        return tx_hashes[0]

class SmartContract:
    """Healthcare smart contract for access control"""
    
    def __init__(self, contract_id: str, patient_id: str):
        self.contract_id = contract_id
        self.patient_id = patient_id
        self.access_rules: Dict[str, Any] = {}
        self.created_at = time.time()
    
    def grant_access(self, requester_id: str, data_types: List[str], duration_hours: int) -> Dict[str, Any]:
        """Grant data access to requester"""
        access_key = f"{requester_id}_{int(time.time())}"
        expiry_time = time.time() + (duration_hours * 3600)
        
        self.access_rules[access_key] = {
            "requester_id": requester_id,
            "data_types": data_types,
            "granted_at": time.time(),
            "expires_at": expiry_time,
            "active": True
        }
        
        return {
            "access_key": access_key,
            "granted": True,
            "expires_at": expiry_time,
            "permissions": data_types
        }
    
    def revoke_access(self, access_key: str) -> bool:
        """Revoke data access"""
        if access_key in self.access_rules:
            self.access_rules[access_key]["active"] = False
            return True
        return False
    
    def validate_access(self, requester_id: str, data_type: str) -> bool:
        """Validate if requester has access to data type"""
        for access_key, rules in self.access_rules.items():
            if (rules["requester_id"] == requester_id and 
                rules["active"] and 
                time.time() < rules["expires_at"] and
                data_type in rules["data_types"]):
                return True
        return False

class ConsensusEngine:
    """Proof of Authority + PBFT Consensus Engine"""
    
    def __init__(self, validators: List[str]):
        self.validators = validators  # List of authorized validator addresses
        self.primary_validator = validators[0] if validators else None
        self.byzantine_tolerance = max(1, (len(validators) - 1) // 3)
        self.consensus_threshold = len(validators) - self.byzantine_tolerance
    
    async def validate_transaction(self, transaction: Transaction) -> bool:
        """Validate transaction using PoA"""
        try:
            # Basic validation
            if not transaction.transaction_id or not transaction.patient_id:
                return False
            
            # Validate transaction signature (simplified)
            if not self._verify_signature(transaction):
                return False
            
            # Healthcare-specific validation
            if transaction.transaction_type == TransactionType.PATIENT_DATA:
                return self._validate_patient_data(transaction)
            elif transaction.transaction_type == TransactionType.ACCESS_GRANT:
                return self._validate_access_grant(transaction)
            
            return True
            
        except Exception as e:
            print(f"Transaction validation error: {e}")
            return False
    
    def _verify_signature(self, transaction: Transaction) -> bool:
        """Simplified signature verification"""
        # In real implementation, use cryptographic signature verification
        expected_sig = hashlib.sha256(f"{transaction.sender}{transaction.data_hash}".encode()).hexdigest()
        return transaction.signature == expected_sig[:32]  # Simplified for MVP
    
    def _validate_patient_data(self, transaction: Transaction) -> bool:
        """Validate patient data transaction"""
        required_fields = ["device_id", "data_type", "encrypted_data"]
        return all(field in transaction.payload for field in required_fields)
    
    def _validate_access_grant(self, transaction: Transaction) -> bool:
        """Validate access grant transaction"""
        required_fields = ["requester_id", "data_types", "duration"]
        return all(field in transaction.payload for field in required_fields)
    
    async def reach_consensus(self, transactions: List[Transaction]) -> Dict[str, Any]:
        """PBFT consensus for transaction validation"""
        consensus_result = {
            "status": ConsensusStatus.PENDING,
            "validated_transactions": [],
            "rejected_transactions": [],
            "validator_votes": {},
            "consensus_time": time.time()
        }
        
        # Validate each transaction
        for tx in transactions:
            if await self.validate_transaction(tx):
                consensus_result["validated_transactions"].append(tx.transaction_id)
            else:
                consensus_result["rejected_transactions"].append(tx.transaction_id)
        
        # Simulate PBFT voting (simplified for MVP)
        votes_needed = self.consensus_threshold
        votes_received = len(self.validators)  # Simulate all validators voting
        
        if votes_received >= votes_needed:
            consensus_result["status"] = ConsensusStatus.VALIDATED
        else:
            consensus_result["status"] = ConsensusStatus.REJECTED
        
        return consensus_result

class BlockchainCore:
    """Main CareChain Blockchain Implementation"""
    
    def __init__(self, validators: List[str]):
        self.chain: List[Block] = []
        self.pending_transactions: List[Transaction] = []
        self.consensus_engine = ConsensusEngine(validators)
        self.smart_contracts: Dict[str, SmartContract] = {}
        self.transaction_pool: Dict[str, Transaction] = {}
        self.validators = validators
        
        # Create genesis block
        self._create_genesis_block()
    
    def _create_genesis_block(self):
        """Create the genesis block"""
        genesis_block = Block(
            block_number=0,
            previous_hash="0" * 64,
            timestamp=time.time(),
            transactions=[],
            merkle_root="",
            validator="genesis",
            signature="genesis_signature"
        )
        genesis_block.merkle_root = genesis_block.calculate_merkle_root()
        self.chain.append(genesis_block)
    
    def get_latest_block(self) -> Block:
        """Get the latest block in the chain"""
        return self.chain[-1]
    
    async def submit_transaction(self, transaction: Transaction) -> Dict[str, Any]:
        """Submit transaction to the blockchain"""
        try:
            # Add to transaction pool
            self.transaction_pool[transaction.transaction_id] = transaction
            self.pending_transactions.append(transaction)
            
            return {
                "transaction_id": transaction.transaction_id,
                "status": "submitted",
                "timestamp": time.time(),
                "message": "Transaction submitted successfully"
            }
            
        except Exception as e:
            return {
                "transaction_id": transaction.transaction_id,
                "status": "failed",
                "error": str(e)
            }
    
    async def mine_block(self, validator_address: str) -> Dict[str, Any]:
        """Mine a new block with pending transactions"""
        if not self.pending_transactions:
            return {"status": "no_transactions", "message": "No pending transactions to mine"}
        
        # Reach consensus on pending transactions
        consensus_result = await self.consensus_engine.reach_consensus(self.pending_transactions)
        
        if consensus_result["status"] != ConsensusStatus.VALIDATED:
            return {"status": "consensus_failed", "result": consensus_result}
        
        # Get validated transactions
        validated_txs = [
            tx for tx in self.pending_transactions 
            if tx.transaction_id in consensus_result["validated_transactions"]
        ]
        
        # Create new block
        new_block = Block(
            block_number=len(self.chain),
            previous_hash=self.get_latest_block().calculate_hash(),
            timestamp=time.time(),
            transactions=validated_txs,
            merkle_root="",
            validator=validator_address,
            signature=""
        )
        
        # Calculate merkle root and block hash
        new_block.merkle_root = new_block.calculate_merkle_root()
        block_hash = new_block.calculate_hash()
        new_block.signature = hashlib.sha256(f"{validator_address}{block_hash}".encode()).hexdigest()
        
        # Add block to chain
        self.chain.append(new_block)
        
        # Clear processed transactions
        processed_tx_ids = set(tx.transaction_id for tx in validated_txs)
        self.pending_transactions = [
            tx for tx in self.pending_transactions 
            if tx.transaction_id not in processed_tx_ids
        ]
        
        return {
            "status": "success",
            "block_number": new_block.block_number,
            "block_hash": block_hash,
            "transactions_processed": len(validated_txs),
            "consensus_result": consensus_result
        }
    
    def deploy_smart_contract(self, patient_id: str) -> str:
        """Deploy smart contract for patient"""
        contract_id = f"contract_{patient_id}_{int(time.time())}"
        contract = SmartContract(contract_id, patient_id)
        self.smart_contracts[contract_id] = contract
        return contract_id
    
    def get_smart_contract(self, contract_id: str) -> Optional[SmartContract]:
        """Get smart contract by ID"""
        return self.smart_contracts.get(contract_id)
    
    def get_transaction_history(self, patient_id: str) -> List[Dict[str, Any]]:
        """Get transaction history for a patient"""
        history = []
        for block in self.chain:
            for tx in block.transactions:
                if tx.patient_id == patient_id:
                    history.append({
                        "transaction_id": tx.transaction_id,
                        "block_number": block.block_number,
                        "timestamp": tx.timestamp,
                        "transaction_type": tx.transaction_type.value,
                        "data_hash": tx.data_hash
                    })
        return history
    
    def get_chain_status(self) -> Dict[str, Any]:
        """Get blockchain status"""
        return {
            "total_blocks": len(self.chain),
            "pending_transactions": len(self.pending_transactions),
            "total_smart_contracts": len(self.smart_contracts),
            "validators": self.validators,
            "latest_block_hash": self.get_latest_block().calculate_hash(),
            "chain_height": len(self.chain) - 1
        }

# Helper functions for transaction creation
def create_patient_data_transaction(
    patient_id: str, 
    device_id: str, 
    data_type: str, 
    encrypted_data: str,
    sender: str
) -> Transaction:
    """Create patient data transaction"""
    transaction_id = f"tx_{patient_id}_{int(time.time())}"
    data_hash = hashlib.sha256(encrypted_data.encode()).hexdigest()
    signature = hashlib.sha256(f"{sender}{data_hash}".encode()).hexdigest()[:32]
    
    return Transaction(
        transaction_id=transaction_id,
        transaction_type=TransactionType.PATIENT_DATA,
        patient_id=patient_id,
        data_hash=data_hash,
        payload={
            "device_id": device_id,
            "data_type": data_type,
            "encrypted_data": encrypted_data
        },
        timestamp=time.time(),
        sender=sender,
        signature=signature
    )

def create_access_grant_transaction(
    patient_id: str,
    requester_id: str,
    data_types: List[str],
    duration_hours: int,
    sender: str
) -> Transaction:
    """Create access grant transaction"""
    transaction_id = f"access_{patient_id}_{requester_id}_{int(time.time())}"
    payload = {
        "requester_id": requester_id,
        "data_types": data_types,
        "duration": duration_hours
    }
    data_hash = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
    signature = hashlib.sha256(f"{sender}{data_hash}".encode()).hexdigest()[:32]
    
    return Transaction(
        transaction_id=transaction_id,
        transaction_type=TransactionType.ACCESS_GRANT,
        patient_id=patient_id,
        data_hash=data_hash,
        payload=payload,
        timestamp=time.time(),
        sender=sender,
        signature=signature
    )

# Global blockchain instance for MVP
blockchain = None

def initialize_blockchain(validators: List[str] = None) -> BlockchainCore:
    """Initialize the global blockchain instance"""
    global blockchain
    if validators is None:
        validators = ["validator_1", "validator_2", "validator_3"]  # Default validators
    blockchain = BlockchainCore(validators)
    return blockchain

def get_blockchain() -> BlockchainCore:
    """Get the global blockchain instance"""
    global blockchain
    if blockchain is None:
        blockchain = initialize_blockchain()
    return blockchain
