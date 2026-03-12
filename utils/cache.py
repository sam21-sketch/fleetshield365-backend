"""
Caching utilities for API responses
"""
from typing import Dict, Any, Optional
from datetime import datetime

# Universal in-memory cache for API responses
api_cache: Dict[str, Any] = {}

CACHE_TTL = {
    "dashboard": 30,    # Dashboard stats: 30 seconds
    "vehicles": 30,     # Vehicles list: 30 seconds
    "drivers": 30,      # Drivers list: 30 seconds
    "inspections": 15,  # Inspections: 15 seconds (more dynamic)
}


def get_cached(cache_type: str, company_id: str) -> Optional[Any]:
    """Get cached data if still valid"""
    cache_key = f"{cache_type}_{company_id}"
    if cache_key in api_cache:
        cached = api_cache[cache_key]
        ttl = CACHE_TTL.get(cache_type, 30)
        if datetime.utcnow().timestamp() - cached["timestamp"] < ttl:
            return cached["data"]
    return None


def set_cached(cache_type: str, company_id: str, data: Any):
    """Cache API response data"""
    cache_key = f"{cache_type}_{company_id}"
    api_cache[cache_key] = {
        "timestamp": datetime.utcnow().timestamp(),
        "data": data
    }


def invalidate_cache(cache_type: str, company_id: str):
    """Invalidate cache when data changes"""
    cache_key = f"{cache_type}_{company_id}"
    if cache_key in api_cache:
        del api_cache[cache_key]


# Legacy functions for backwards compatibility
dashboard_cache = api_cache
CACHE_TTL_SECONDS = 30


def get_cached_stats(company_id: str) -> Optional[dict]:
    return get_cached("dashboard", company_id)


def set_cached_stats(company_id: str, data: dict):
    set_cached("dashboard", company_id, data)
