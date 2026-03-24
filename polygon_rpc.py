"""Consultas on-chain a Polygon via Web3 y RPC directo."""
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

import aiohttp
from web3 import Web3

from config import POLYGON_RPC_URL, DEFI_CONTRACTS, CEX_LABELS

logger = logging.getLogger(__name__)

# Polygon usa ~2s por bloque → ~43200 bloques/día
BLOCKS_PER_DAY = 43_200
ETHERSCAN_POLY = "https://api.polygonscan.com/api"  # sin API key, límite bajo

_w3 = Web3(Web3.HTTPProvider(POLYGON_RPC_URL))


def _to_checksum(addr: str) -> str:
    try:
        return Web3.to_checksum_address(addr)
    except Exception:
        return addr


class PolygonClient:
    def __init__(self) -> None:
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=20)
            )
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def _rpc(self, method: str, params: list) -> Optional[dict]:
        session = await self._get_session()
        payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        try:
            async with session.post(POLYGON_RPC_URL, json=payload) as resp:
                data = await resp.json(content_type=None)
                return data.get("result")
        except Exception as exc:
            logger.error("RPC %s error: %s", method, exc)
            return None

    # ── Bloque actual ─────────────────────────────────────────────────────────

    async def get_latest_block(self) -> int:
        result = await self._rpc("eth_blockNumber", [])
        if result:
            return int(result, 16)
        return 0

    # ── Primer bloque de la wallet (aproximado) ───────────────────────────────

    async def get_wallet_first_tx_timestamp(self, wallet: str) -> Optional[datetime]:
        """
        Estrategia: busca el primer bloque donde hay actividad via Polygonscan
        (sin API key → resultados limitados a 10k txs, suficiente para wallets nuevas).
        """
        session = await self._get_session()
        addr = wallet.lower()
        params = {
            "module": "account",
            "action": "txlist",
            "address": addr,
            "startblock": 0,
            "endblock": 99999999,
            "sort": "asc",
            "page": 1,
            "offset": 1,
        }
        try:
            async with session.get(ETHERSCAN_POLY, params=params) as resp:
                data = await resp.json(content_type=None)
            txs = data.get("result", [])
            if txs and isinstance(txs, list) and len(txs) > 0:
                ts = int(txs[0].get("timeStamp", 0))
                if ts:
                    return datetime.fromtimestamp(ts, tz=timezone.utc)
        except Exception as exc:
            logger.debug("Polygonscan first tx error %s: %s", wallet, exc)

        # Fallback: comprueba si el nonce es 0 (wallet nunca ha enviado tx)
        try:
            nonce_hex = await self._rpc(
                "eth_getTransactionCount", [_to_checksum(wallet), "latest"]
            )
            if nonce_hex and int(nonce_hex, 16) == 0:
                # Wallet sin actividad de envío → muy nueva
                return datetime.now(tz=timezone.utc)
        except Exception:
            pass
        return None

    # ── Historial de transacciones ────────────────────────────────────────────

    async def get_tx_history(self, wallet: str, limit: int = 100) -> list[dict]:
        session = await self._get_session()
        addr = wallet.lower()
        params = {
            "module": "account",
            "action": "txlist",
            "address": addr,
            "startblock": 0,
            "endblock": 99999999,
            "sort": "desc",
            "page": 1,
            "offset": limit,
        }
        try:
            async with session.get(ETHERSCAN_POLY, params=params) as resp:
                data = await resp.json(content_type=None)
            txs = data.get("result", [])
            return txs if isinstance(txs, list) else []
        except Exception as exc:
            logger.debug("Polygonscan tx list error %s: %s", wallet, exc)
            return []

    # ── Detectar interacción con DeFi ─────────────────────────────────────────

    async def has_defi_activity(self, wallet: str) -> bool:
        txs = await self.get_tx_history(wallet, limit=200)
        defi_addrs = {v.lower() for v in DEFI_CONTRACTS.values()}
        for tx in txs:
            to_addr = (tx.get("to") or "").lower()
            from_addr = (tx.get("from") or "").lower()
            if to_addr in defi_addrs or from_addr in defi_addrs:
                return True
        return False

    # ── Detectar fondos desde CEX ─────────────────────────────────────────────

    async def get_funding_source(self, wallet: str) -> Optional[str]:
        """
        Devuelve etiqueta del CEX si el primer depósito de MATIC/USDC proviene de uno.
        """
        txs = await self.get_tx_history(wallet, limit=50)
        # Buscamos también token transfers (ERC-20)
        session = await self._get_session()
        token_txs: list[dict] = []
        try:
            params = {
                "module": "account",
                "action": "tokentx",
                "address": wallet.lower(),
                "startblock": 0,
                "endblock": 99999999,
                "sort": "asc",
                "page": 1,
                "offset": 10,
            }
            async with session.get(ETHERSCAN_POLY, params=params) as resp:
                data = await resp.json(content_type=None)
            result = data.get("result", [])
            token_txs = result if isinstance(result, list) else []
        except Exception:
            pass

        all_txs = txs + token_txs
        cex_lower = {k.lower(): v for k, v in CEX_LABELS.items()}
        for tx in sorted(all_txs, key=lambda x: int(x.get("timeStamp", 0))):
            sender = (tx.get("from") or "").lower()
            if sender in cex_lower:
                return cex_lower[sender]
        return None

    # ── Detectar wallets con mismo origen ────────────────────────────────────

    async def get_funding_address(self, wallet: str) -> Optional[str]:
        """Devuelve la dirección que financió esta wallet (primer tx recibida)."""
        txs = await self.get_tx_history(wallet, limit=50)
        received = [
            tx for tx in txs
            if (tx.get("to") or "").lower() == wallet.lower()
        ]
        if not received:
            return None
        # Ordena ASC para encontrar la primera
        received.sort(key=lambda x: int(x.get("timeStamp", 0)))
        return (received[0].get("from") or "").lower()

    # ── Calcular edad de wallet ───────────────────────────────────────────────

    async def get_wallet_age_days(self, wallet: str) -> float:
        first_ts = await self.get_wallet_first_tx_timestamp(wallet)
        if first_ts is None:
            return 0.0
        now = datetime.now(tz=timezone.utc)
        return (now - first_ts).total_seconds() / 86400

    # ── Balance USDC en Polygon ───────────────────────────────────────────────

    async def get_usdc_balance(self, wallet: str) -> float:
        """Balance de USDC.e / USDC nativo en Polygon."""
        # USDC nativo en Polygon: 0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359
        # USDC.e bridgeado:       0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174
        usdc_contracts = [
            "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359",
            "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
        ]
        total = 0.0
        # ABI mínimo para balanceOf
        abi = [{"inputs": [{"name": "account", "type": "address"}],
                "name": "balanceOf",
                "outputs": [{"name": "", "type": "uint256"}],
                "stateMutability": "view",
                "type": "function"}]
        for addr in usdc_contracts:
            try:
                contract = _w3.eth.contract(
                    address=Web3.to_checksum_address(addr), abi=abi
                )
                bal = await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda c=contract, w=wallet: c.functions.balanceOf(
                        Web3.to_checksum_address(w)
                    ).call(),
                )
                total += bal / 1e6  # USDC tiene 6 decimales
            except Exception as exc:
                logger.debug("USDC balance error %s: %s", wallet, exc)
        return total
