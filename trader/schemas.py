from pydantic import BaseModel
from typing import Optional

class BalanceIn(BaseModel):
    asset: str
    free: float
    locked: float = 0.0

class OrderIn(BaseModel):
    symbol: str
    side: str              # BUY/SELL
    type: str              # LIMIT/MARKET
    price: Optional[float]
    qty: float
    client_order_id: Optional[str] = None
    exch_order_id: Optional[str] = None
    status: str = "NEW"

class TradeIn(BaseModel):
    symbol: str
    side: str
    price: float
    qty: float
    fee_asset: Optional[str] = None
    fee: float = 0.0
    order_client_id: Optional[str] = None
