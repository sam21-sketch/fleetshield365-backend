"""
Authentication utilities - JWT, password hashing, user validation
"""
import bcrypt
import jwt
import re
from datetime import datetime, timedelta
from typing import Optional
from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from bson import ObjectId
import os

from utils.database import db

# JWT Configuration
SECRET_KEY = os.environ.get('JWT_SECRET', 'fleetguard-secret-key-2025')
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_HOURS = 24

security = HTTPBearer()


def get_password_hash(password: str) -> str:
    """Hash a password using bcrypt"""
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a password against its hash"""
    return bcrypt.checkpw(plain_password.encode('utf-8'), hashed_password.encode('utf-8'))


def create_access_token(data: dict, expires_delta: timedelta = None) -> str:
    """Create a JWT access token"""
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(hours=ACCESS_TOKEN_EXPIRE_HOURS))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Dependency to get the current authenticated user"""
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = payload.get("sub")
        if user_id is None:
            raise HTTPException(status_code=401, detail="Invalid token")
        user = await db.users.find_one({"_id": ObjectId(user_id)})
        if user is None:
            raise HTTPException(status_code=401, detail="User not found")
        user['id'] = str(user['_id'])
        return user
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")


async def generate_unique_username(name: str, company_id: str) -> str:
    """Generate a unique username from the person's name with random numbers"""
    import random
    
    # Clean the name: lowercase, remove special chars, keep only first name
    clean_name = re.sub(r'[^a-z0-9]', '', name.lower().strip().split()[0] if name.strip() else 'user')
    
    if not clean_name:
        clean_name = "user"
    
    # Try base username first, then add random numbers
    username = clean_name
    attempts = 0
    max_attempts = 50
    
    while await db.users.find_one({"username": username, "company_id": company_id}):
        # Generate random 1-2 digit number (1-99)
        random_num = random.randint(1, 99)
        username = f"{clean_name}{random_num}"
        attempts += 1
        if attempts >= max_attempts:
            # Fallback to 3 digit random if too many collisions
            username = f"{clean_name}{random.randint(100, 999)}"
            break
    
    return username
