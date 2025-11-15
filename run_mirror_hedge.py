import os
import asyncio
from typing import Any, Dict, List, Optional, Tuple

import yaml
from dotenv import load_dotenv
from loguru import logger

import httpx
from edgex_sdk import Client as EdgeXClient, OrderSide as SDKOrderSide


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "on")


def _extract_rows_generic(container: Any) -> list:
    """Generic extraction of row arrays from diverse SDK responses.

    - Handles dict: unwrap nested data; then try keys: dataList, rows, list, orders, items, records, positions, positionList
    - Handles list: return as-is
    - Handles attribute containers: tries same keys via getattr
    """
    if isinstance(container, list):
        return container

    def _dict_rows(d: dict) -> list:
        keys = [
            "dataList",
            "rows",
            "list",
            "orders",
            "items",
            "records",
            "positions",
            "positionList",
        ]
        for k in keys:
            v = d.get(k)
            if isinstance(v, list):
                return v
        return []

    # Dict path
    if isinstance(container, dict):
        cur: Any = container
        for _ in range(2):  # unwrap nested data up to 2 layers
            if isinstance(cur, dict) and isinstance(cur.get("data"), (dict, list)):
                cur = cur.get("data")
        if isinstance(cur, list):
            return cur
        if isinstance(cur, dict):
            rows = _dict_rows(cur)
            if rows:
                return rows

    # Attribute path
    for key in ("data", "rows", "list", "dataList", "items", "records", "positions", "positionList"):
        try:
            v = getattr(container, key)
        except Exception:
            v = None
        if isinstance(v, list):
            return v
        if isinstance(v, dict):
            rows = _dict_rows(v)
            if rows:
                return rows
    return []


async def get_market_rules(base_url: str, contract_id: str) -> Dict[str, float]:
    """Fetch market rules (size_step, price_tick, min_size) via public API.

    Returns empty dict on failure. Keys may include: size_step, price_tick, min_size
    """
    base = base_url.rstrip("/")
    url = f"{base}/api/v1/public/meta/getMetaData"
    rules: Dict[str, float] = {}
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(url)
            r.raise_for_status()
            body = r.json()
            data = body.get("data") if isinstance(body, dict) else None
            if not isinstance(data, dict):
                return rules
            clist = data.get("contractList") or []
            tgt: Optional[Dict[str, Any]] = None
            for c in clist:
                try:
                    if str(c.get("contractId")) == str(contract_id):
                        tgt = c
                        break
                except Exception:
                    continue
            if not isinstance(tgt, dict):
                return rules

            def _to_float(x: Any) -> Optional[float]:
                try:
                    if x is None:
                        return None
                    return float(str(x))
                except Exception:
                    return None

            size_step = _to_float(tgt.get("stepSize") or tgt.get("quantityStep") or tgt.get("sizeStep"))
            price_tick = _to_float(tgt.get("tickSize") or tgt.get("priceTick") or tgt.get("priceStep"))
            min_size = _to_float(tgt.get("minOpenSize") or tgt.get("minOrderSize") or tgt.get("minSize"))
            if size_step and size_step > 0:
                rules["size_step"] = size_step
            if price_tick and price_tick > 0:
                rules["price_tick"] = price_tick
            if min_size and min_size > 0:
                rules["min_size"] = min_size
    except Exception:
        return rules
    return rules


async def get_ticker_mid_price(base_url: str, contract_id: str) -> Optional[float]:
    """Fetch mid price using public ticker endpoint. Returns None on failure.

    Tries common fields: bestBidPrice/bestAskPrice, bid/ask, lastPrice, price.
    """
    base = (base_url or "").rstrip("/")
    url = f"{base}/api/v1/public/quote/getTicker?contractId={contract_id}"
    try:
        async with httpx.AsyncClient(timeout=6.0, headers={"Accept": "application/json"}) as client:
            r = await client.get(url)
            r.raise_for_status()
            body = r.json()
            data = body.get("data") if isinstance(body, dict) else None
            if not isinstance(data, dict):
                return None
            # Common fields
            bid = data.get("bestBidPrice") or data.get("bid") or data.get("bestBid")
            ask = data.get("bestAskPrice") or data.get("ask") or data.get("bestAsk")
            last = data.get("lastPrice") or data.get("price") or data.get("last")

            def _f(x: Any) -> Optional[float]:
                try:
                    if x is None:
                        return None
                    return float(str(x))
                except Exception:
                    return None

            b = _f(bid)
            a = _f(ask)
            if b and a and b > 0 and a > 0:
                return (b + a) / 2.0
            l = _f(last)
            return l if (l and l > 0) else None
    except Exception:
        return None


async def fetch_unrealized_pnl(
    client: EdgeXClient, account_id: int, contract_id: str
) -> Optional[float]:
    """EdgeX SDKに合わせた厳密版: account.get_account_positions() を無引数で呼ぶ。
    未実現P/Lは data.positionAssetList[].unrealizePnl を、contractId 一致の要素から取得する。
    見つからなければ None。
    """
    try:
        if not hasattr(client, "account") or not hasattr(client.account, "get_account_positions"):
            return None
        resp = await client.account.get_account_positions()
    except Exception:
        if _env_bool("MIRROR_DEBUG", False):
            logger.debug("pnl fetch: get_account_positions() failed accountId={} contractId={}", account_id, contract_id)
        return None

    if not isinstance(resp, dict) or resp.get("code") != "SUCCESS":
        return None
    data = resp.get("data", {}) if isinstance(resp, dict) else {}
    # positionAssetList から contractId 一致を抽出
    pos_assets = data.get("positionAssetList") or []
    if not isinstance(pos_assets, list):
        pos_assets = []
    target_asset: Optional[dict] = None
    for a in pos_assets:
        if isinstance(a, dict) and str(a.get("contractId")) == str(contract_id):
            target_asset = a
            break
    if target_asset is None:
        return None
    # unrealizePnl（または unrealizedPnl）を取得
    v = target_asset.get("unrealizePnl")
    if v is None:
        v = target_asset.get("unrealizedPnl")
    try:
        return float(str(v)) if v is not None else None
    except Exception:
        return None

def round_size_to_step(qty: float, rules: Dict[str, float]) -> float:
    if qty <= 0:
        return 0.0
    step_env = os.getenv("EDGEX_SIZE_STEP")
    step = None
    try:
        if step_env:
            step = float(step_env)
        elif "size_step" in rules:
            step = float(rules["size_step"])  # type: ignore[arg-type]
    except Exception:
        step = None
    if step and step > 0:
        # 切り捨て丸め（発注は現在量以下に限定。最小ロットへの繰り上げはしない）
        units = int(qty / step)
        qty = units * step
    if qty <= 0:
        return 0.0
    min_sz = None
    try:
        if "min_size" in rules:
            min_sz = float(rules["min_size"])  # type: ignore[arg-type]
    except Exception:
        min_sz = None
    if min_sz and qty < min_sz:
        # 取引所の最小サイズに満たない調整は発注しない（往復振動を防止）
        return 0.0
    return qty


async def fetch_net_position(client: EdgeXClient, account_id: int, contract_id: str) -> float:
    """Return net position size (positive=long, negative=short) for account+contract.

    This function tries several common SDK method patterns and normalizes the result.
    Returns 0.0 if unavailable.
    """
    # Try candidate methods in preferred order
    candidates: List[Tuple[str, str]] = [
        ("account", "get_position_page"),
        ("account", "get_positions"),
        ("account", "get_account_positions"),
        ("position", "get_position_page"),
        ("position", "get_positions"),
        ("", "get_position_page"),
        ("", "get_positions"),
    ]

    resp: Any = None
    chosen: tuple[str, str] | None = None
    for ns, fn in candidates:
        try:
            target = getattr(client, ns) if ns else client
            if hasattr(target, fn):
                method = getattr(target, fn)
                # signature debug
                if _env_bool("MIRROR_DEBUG", False):
                    try:
                        import inspect as _inspect
                        sig = _inspect.signature(method)
                        logger.debug("refpos signature: method={}.{} params={}", ns or "root", fn, list(sig.parameters.keys()))
                    except Exception:
                        pass
                # Try multiple param shapes
                params_variants = [
                    {"account_id": account_id, "contract_id": str(contract_id), "size": 200},
                    {"accountId": str(account_id), "contractId": str(contract_id), "size": "200"},
                    {"accountId": str(account_id), "contractIds": [str(contract_id)], "size": "200"},
                    {"account_id": account_id, "contract_id_list": [str(contract_id)], "size": 200},
                    {"accountId": str(account_id), "contractIdList": [str(contract_id)], "size": "200"},
                    {"params": {"accountId": str(account_id), "contractId": str(contract_id), "size": "200"}},
                    {"params": {"accountId": str(account_id), "contractIds": [str(contract_id)], "size": "200"}},
                    {"params": {"accountId": str(account_id), "contractIdList": [str(contract_id)], "size": "200"}},
                ]
                for pv in params_variants:
                    try:
                        if _env_bool("MIRROR_DEBUG", False):
                            logger.debug("refpos try: method={}.{} params_keys={}", ns or "root", fn, list(pv.keys()))
                        resp = await method(**pv)  # type: ignore[arg-type]
                        chosen = (ns, fn)
                        break
                    except TypeError:
                        if _env_bool("MIRROR_DEBUG", False):
                            logger.debug("refpos typeerror: method={}.{} params_keys={}", ns or "root", fn, list(pv.keys()))
                        continue
                if resp is None:
                    # last resort: minimal account-only; some APIs require no contract filter
                    try:
                        if _env_bool("MIRROR_DEBUG", False):
                            logger.debug("refpos fallback: method={}.{} params_keys=[accountId,size]", ns or "root", fn)
                        resp = await method(accountId=str(account_id), size="200")
                        chosen = (ns, fn)
                    except Exception:
                        pass
                if resp is None:
                    # ultimate fallback: zero-arg (some SDKs expose account-bound clients)
                    try:
                        if _env_bool("MIRROR_DEBUG", False):
                            logger.debug("refpos zerocall: method={}.{} ()", ns or "root", fn)
                        resp = await method()
                        chosen = (ns, fn)
                    except Exception:
                        pass
                break
        except Exception:
            if _env_bool("MIRROR_DEBUG", False):
                logger.debug("refpos skip: method={}.{} error", ns or "root", fn)
            continue

    if resp is None:
        return 0.0

    try:
        rows = _extract_rows_generic(resp)
    except Exception:
        rows = []

    # Optional debug: dump summary
    if _env_bool("MIRROR_DEBUG", False):
        try:
            logger.debug(
                "refpos raw: method={} rows_len={} sample={}",
                (".".join(filter(None, chosen)) if chosen else None),
                (len(rows) if isinstance(rows, list) else None),
                ([{k: r.get(k) for k in ("contractId","symbol","size","positionSize","longSize","shortSize","side","positionSide")} for r in (rows[:2] if isinstance(rows, list) else [])]),
            )
        except Exception:
            pass
        # file dump for diagnostics
        try:
            if _env_bool("MIRROR_DEBUG", False):
                import json as _json
                os.makedirs("logs", exist_ok=True)
                with open(os.path.join("logs", "refpos_raw.json"), "w", encoding="utf-8") as f:
                    def _safe(o):
                        try:
                            _json.dumps(o)
                            return o
                        except Exception:
                            try:
                                return getattr(o, "__dict__", str(o))
                            except Exception:
                                return str(o)
                    _json.dump(_safe(resp), f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    net = 0.0
    for r in rows or []:
        try:
            if not isinstance(r, dict):
                continue
            cid = str(r.get("contractId") or r.get("symbol") or r.get("contract_id") or "")
            if cid and cid != str(contract_id):
                if _env_bool("MIRROR_DEBUG", False):
                    logger.debug("refpos skip row: cid_mismatch row_cid={} target_cid={}", cid, contract_id)
                continue

            # Common fields: prefer openSize (already signed), else long/short split, else size+side
            open_sz = r.get("openSize") or r.get("positionOpenSize")
            if open_sz is not None:
                try:
                    net += float(open_sz)
                    if _env_bool("MIRROR_DEBUG", False):
                        logger.debug("refpos add: cid={} openSize={} net={}", cid, open_sz, net)
                    continue
                except Exception:
                    pass
            long_sz = r.get("longSize") or r.get("holdLong") or r.get("buySize")
            short_sz = r.get("shortSize") or r.get("holdShort") or r.get("sellSize")
            if long_sz is not None or short_sz is not None:
                try:
                    l = float(long_sz or 0)
                    s = float(short_sz or 0)
                    net += (l - s)
                    continue
                except Exception:
                    pass

            size = r.get("positionSize") or r.get("size") or r.get("pos") or r.get("netSize")
            side = (r.get("positionSide") or r.get("side") or r.get("holdDirection") or "").upper()
            if size is not None:
                try:
                    sz = float(size)
                except Exception:
                    sz = 0.0
                if side in ("SHORT", "SELL"):
                    sz = -abs(sz)
                elif side in ("LONG", "BUY"):
                    sz = abs(sz)
                elif r.get("isShort") is True:
                    sz = -abs(sz)
                net += sz
            if _env_bool("MIRROR_DEBUG", False):
                try:
                    logger.debug("refpos add: cid={} l={} s={} size={} side={} net={}",
                                 cid,
                                 (long_sz if 'long_sz' in locals() else None),
                                 (short_sz if 'short_sz' in locals() else None),
                                 size,
                                 side,
                                 net)
                except Exception:
                    pass
        except Exception:
            continue
    return float(net)


async def discover_contracts_from_ref(client: EdgeXClient, account_id: int) -> List[str]:
    """参照アカウントの保有ポジションから、ネットが非ゼロの contractId を自動検出して返す。
    SDKの実装差に耐えるよう、複数の代表的メソッドから取得を試みる。
    """
    # 代表候補
    candidates: List[Tuple[str, str]] = [
        ("account", "get_position_page"),
        ("account", "get_positions"),
        ("account", "get_position_list"),
        ("account", "get_account_positions"),
        ("position", "get_position_page"),
        ("position", "get_positions"),
        ("position", "list_positions"),
        ("", "get_position_page"),
        ("", "get_positions"),
        ("", "list_positions"),
    ]

    resp: Any = None
    for ns, fn in candidates:
        try:
            target = getattr(client, ns) if ns else client
            if hasattr(target, fn):
                method = getattr(target, fn)
                # signature debug
                if _env_bool("MIRROR_DEBUG", False):
                    try:
                        import inspect as _inspect
                        sig = _inspect.signature(method)
                        logger.debug("discover signature: method={}.{} params={}", ns or "root", fn, list(sig.parameters.keys()))
                    except Exception:
                        pass
                # 試行順: named -> camel -> single-dict。size/pageも試す
                params_variants = [
                    {"account_id": account_id, "size": 200, "page": 1},
                    {"accountId": str(account_id), "size": "200", "page": "1"},
                    {"params": {"accountId": str(account_id), "size": "200", "page": "1"}},
                    {"account_id": account_id},
                    {"accountId": str(account_id)},
                    {"params": {"accountId": str(account_id)}},
                ]
                for pv in params_variants:
                    try:
                        if _env_bool("MIRROR_DEBUG", False):
                            logger.debug("discover try: method={}.{} params_keys={}", ns or "root", fn, list(pv.keys()))
                        resp = await method(**pv)  # type: ignore[arg-type]
                        break
                    except TypeError:
                        if _env_bool("MIRROR_DEBUG", False):
                            logger.debug("discover typeerror: method={}.{} params_keys={}", ns or "root", fn, list(pv.keys()))
                        continue
                if resp is None:
                    # ultimate fallback: zero-arg
                    try:
                        if _env_bool("MIRROR_DEBUG", False):
                            logger.debug("discover zerocall: method={}.{} ()", ns or "root", fn)
                        resp = await method()
                    except Exception:
                        pass
                break
        except Exception:
            if _env_bool("MIRROR_DEBUG", False):
                logger.debug("discover skip: method={}.{} error", ns or "root", fn)
            continue

    try:
        rows = _extract_rows_generic(resp)
    except Exception:
        rows = []

    results: List[str] = []
    for r in rows or []:
        try:
            if not isinstance(r, dict):
                continue
            cid = r.get("contractId") or r.get("symbol") or r.get("contract_id")
            if cid is None:
                continue
            cid_str = str(cid)

            # ネット計算（fetch_net_position と同様のキー群）
            open_sz = r.get("openSize") or r.get("positionOpenSize")
            long_sz = r.get("longSize") or r.get("holdLong") or r.get("buySize")
            short_sz = r.get("shortSize") or r.get("holdShort") or r.get("sellSize")
            net_val: float | None = None
            if open_sz is not None:
                try:
                    net_val = float(open_sz)
                except Exception:
                    net_val = None
            if long_sz is not None or short_sz is not None:
                try:
                    net_val = float(long_sz or 0) - float(short_sz or 0)
                except Exception:
                    net_val = None
            if net_val is None:
                size = r.get("positionSize") or r.get("size") or r.get("pos") or r.get("netSize")
                side = (r.get("positionSide") or r.get("side") or r.get("holdDirection") or "").upper()
                try:
                    sz = float(size) if size is not None else 0.0
                except Exception:
                    sz = 0.0
                if side in ("SHORT", "SELL") or r.get("isShort") is True:
                    net_val = -abs(sz)
                elif side in ("LONG", "BUY"):
                    net_val = abs(sz)
                else:
                    net_val = sz

            if abs(float(net_val or 0.0)) > 1e-12 and cid_str not in results:
                results.append(cid_str)
            elif _env_bool("MIRROR_DEBUG", False):
                logger.debug("discover skip row: cid={} net={} (zero or duplicate)", cid_str, net_val)
        except Exception:
            continue
    if _env_bool("MIRROR_DEBUG", False):
        logger.debug("discover result: contracts={}", results)
    if _env_bool("MIRROR_DEBUG", False):
        try:
            logger.debug("discover result: contracts={}", results)
            import json as _json
            os.makedirs("logs", exist_ok=True)
            with open(os.path.join("logs", "discover_raw.json"), "w", encoding="utf-8") as f:
                def _safe(o):
                    try:
                        _json.dumps(o)
                        return o
                    except Exception:
                        try:
                            return getattr(o, "__dict__", str(o))
                        except Exception:
                            return str(o)
                _json.dump(_safe(resp), f, ensure_ascii=False, indent=2)
        except Exception:
            pass
    return results


async def place_market_order(client: EdgeXClient, contract_id: str, side: str, size: float) -> Dict[str, Any]:
    """Place market order using common SDK method names.

    Returns normalized dict with at least orderId if available.
    """
    side_enum = SDKOrderSide.BUY if side.upper() == "BUY" else SDKOrderSide.SELL
    payload = {"contract_id": str(contract_id), "size": str(size), "side": side_enum.value if hasattr(side_enum, "value") else str(side_enum)}

    # Preferred simple methods
    for ns, fn in (("", "create_market_order"), ("order", "create_market_order")):
        try:
            target = getattr(client, ns) if ns else client
            if hasattr(target, fn):
                method = getattr(target, fn)
                try:
                    return await method(contract_id=str(contract_id), size=str(size), side=side_enum)  # type: ignore[arg-type]
                except TypeError:
                    return await method(**payload)
        except Exception:
            continue

    # Fallback: generic create_order with MARKET type
    order_kwargs = {
        "contract_id": str(contract_id),
        "size": str(size),
        "side": side_enum,
        "orderType": "MARKET",
    }
    for ns, fn in (("", "create_order"), ("order", "create_order")):
        try:
            target = getattr(client, ns) if ns else client
            if hasattr(target, fn):
                method = getattr(target, fn)
                try:
                    return await method(**order_kwargs)
                except TypeError:
                    # try snake_case
                    order_kwargs_alt = {
                        "contract_id": str(contract_id),
                        "size": str(size),
                        "side": side_enum,
                        "order_type": "MARKET",
                    }
                    return await method(**order_kwargs_alt)
        except Exception:
            continue

    raise RuntimeError("SDK does not expose market order method")


 


async def mirror_once(
    client_own: EdgeXClient,
    base_url: str,
    ref_account_id: int,
    own_account_id: int,
    contract_id: str,
    step_cap: float,
    *,
    ref_client: Optional[EdgeXClient] = None,
    notional_trigger_usd: float = 0.0,
) -> None:
    # rules and price for rounding and optional notional guards
    rules = await get_market_rules(base_url, contract_id)

    # Fetch net positions
    if ref_client is None:
        raise RuntimeError("reference client is not configured (REF_STARK_PRIVATE_KEY required)")
    ref_net = await fetch_net_position(ref_client, ref_account_id, contract_id)
    own_net = await fetch_net_position(client_own, own_account_id, contract_id)
    # Hedge: aim for own_net = -ref_net → gap = (-ref_net) - own_net
    gap = (-ref_net) - own_net

    logger.debug("mirror: cid={} ref={} own={} gap={}", contract_id, ref_net, own_net, gap)
    # ポリシー:
    # - 参照口座が含み益(>0): 既存ヘッジは解除（自口座=0）。このとき band は無視する。
    # - 参照口座が含み損(<=0): |PnL| >= しきい値(MIRROR_NOTIONAL_TRIGGER_USD)のときのみ新規ヘッジ。
    # - PnLが取得できないとき: 何もしない（安全側）。
    ref_pnl = await fetch_unrealized_pnl(ref_client, ref_account_id, str(contract_id))
    if _env_bool("MIRROR_DEBUG", False):
        logger.debug("pnl gate: ref_account_id={} cid={} ref_pnl={}", ref_account_id, contract_id, ref_pnl)
    # Always show main account PnL even when debug is off
    logger.info("pnl ref: account_id={} cid={} pnl={}", ref_account_id, contract_id, (ref_pnl if ref_pnl is not None else "N/A"))
    # 発注ゲートの詳細を可視化
    try:
        trig = float(notional_trigger_usd or 0.0)
    except Exception:
        trig = 0.0
    if ref_pnl is not None:
        is_profit = (ref_pnl > 0)
        abs_pnl = abs(float(ref_pnl))
        triggered = (not is_profit) and (trig <= 0.0 or abs_pnl >= trig)
        logger.info(
            "gate detail: profit={} abs_pnl={} trigger_usd={} triggered={}",
            is_profit,
            abs_pnl,
            trig,
            triggered,
        )
    if ref_pnl is None:
        return
    if ref_pnl > 0:
        target_gap = 0.0 - own_net
        raw_close = abs(target_gap)
        close_sz = round_size_to_step(raw_close, rules)
        if close_sz <= 0:
            return
        side = "BUY" if target_gap > 0 else "SELL"
        # 予想後ポジションを事前評価（絶対値が減らない注文はスキップして振動防止）
        predicted_net = own_net + close_sz if side == "BUY" else own_net - close_sz
        if abs(predicted_net) >= abs(own_net) - 1e-12:
            return
        res = await place_market_order(client_own, str(contract_id), side, close_sz)
        logger.info("mirror close-hedge: cid={} side={} size={} pnl_ref={} -> {}", contract_id, side, close_sz, ref_pnl, res)
        return

    # 含み損（負またはゼロ）側: しきい値のみ評価して新規/調整
    if notional_trigger_usd and notional_trigger_usd > 0:
        if abs(float(ref_pnl)) < notional_trigger_usd:
            return

    size = abs(gap)
    if step_cap and step_cap > 0:
        size = min(size, step_cap)
    size = round_size_to_step(size, rules)
    if size <= 0:
        return

    side = "BUY" if gap > 0 else "SELL"  # move own towards -ref
    res = await place_market_order(client_own, str(contract_id), side, size)
    logger.info("mirror order: cid={} side={} size={} -> {}", contract_id, side, size, res)


async def auth_check_if_required(cfg: Dict[str, Any], api_id: str) -> None:
    import httpx as _httpx
    # Allow skipping auth via env (ops override)
    if _env_bool("EDGEX_AUTH_SKIP", False):
        logger.warning("認証チェックをスキップします（EDGEX_AUTH_SKIP=1）")
        return

    default_auth_url = "https://script.google.com/macros/s/AKfycbz5qTzBD62-FRdRwA0qBzxPy6fGj3fuuRwx4fQ0cNj-qmLtWwOqo9UZDnc0tv31ezMl/exec"
    auth_url = cfg.get("auth_url") or default_auth_url
    acct_str = str(api_id)
    logger.info("認証チェック開始: url={} account_id={}", auth_url, acct_str)

    # Configurable timeout and retries
    try:
        timeout_sec = float(os.getenv("EDGEX_AUTH_TIMEOUT_SEC", str(cfg.get("auth_timeout_sec", 8.0))))
    except Exception:
        timeout_sec = 8.0
    allow_on_timeout = _env_bool("EDGEX_AUTH_ALLOW_ON_TIMEOUT", False)

    attempts = 4
    backoff = 0.8
    last_err: Exception | None = None
    for i in range(attempts):
        try:
            async with _httpx.AsyncClient(timeout=_httpx.Timeout(timeout_sec), headers={"Accept": "application/json"}, follow_redirects=True) as c:
                r = await c.get(auth_url, params={"accountId": acct_str})
                r.raise_for_status()
                body = r.json()
                allowed_raw = body.get("allowed") if isinstance(body, dict) else None
                allowed = str(allowed_raw).lower() in ("1", "true", "yes")
                if not allowed:
                    logger.error("認証されていないアカウントIDです: account_id={} / 認証してください: {}?accountId={}", acct_str, auth_url, acct_str)
                    raise SystemExit(f"認証NG: account_id={acct_str}")
                logger.info("認証OK: account_id={}", acct_str)
                return
        except Exception as e:
            last_err = e
            # retry on timeouts and transient network errors
            msg = str(e)
            if "Timeout" in msg or isinstance(e, (_httpx.ReadTimeout, _httpx.ConnectTimeout, _httpx.HTTPError)):
                if i < attempts - 1:
                    await asyncio.sleep(7.0)
                    backoff = min(backoff * 1.8, 6.0)
                    continue
            break

    # Exhausted
    if allow_on_timeout and last_err is not None:
        logger.warning("認証サーバに到達できません（タイムアウト扱い）。許可して続行します: {}", last_err)
        return
    raise SystemExit(f"認証サーバ接続失敗: {last_err}")


async def main() -> None:
    load_dotenv()
    # Logs directory
    try:
        os.makedirs("logs", exist_ok=True)
        logger.add(
            os.path.join("logs", "run_mirror_hedge.log"),
            level="DEBUG",
            rotation="10 MB",
            retention="14 days",
            encoding="utf-8",
            enqueue=True,
            backtrace=False,
            diagnose=False,
            format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<8} | {name}:{function}:{line} - {message}",
        )
    except Exception:
        pass

    # Optional config file
    try:
        with open("configs/edgex.yaml", "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
    except FileNotFoundError:
        cfg = {}

    base_url = os.getenv("EDGEX_BASE_URL") or cfg.get("base_url") or "https://pro.edgex.exchange"
    api_id = (
        os.getenv("EDGEX_ACCOUNT_ID")
        or os.getenv("EDGEX_API_ID")
        or cfg.get("account_id")
        or cfg.get("api_id")
    )
    sdk_key = os.getenv("EDGEX_STARK_PRIVATE_KEY") or os.getenv("EDGEX_L2_KEY")

    ref_id_env = os.getenv("REF_ACCOUNT_ID") or os.getenv("MIRROR_REF_ACCOUNT_ID") or cfg.get("ref_account_id")
    if not ref_id_env:
        raise SystemExit("REF_ACCOUNT_ID が未設定です（参照アカウントID）")

    # symbols: デフォルトは自動検出。環境変数は強制時のみ優先。
    sym_single = os.getenv("EDGEX_CONTRACT_ID") or cfg.get("contract_id") or os.getenv("EDGEX_SYMBOL") or cfg.get("symbol")
    sym_list_env = os.getenv("EDGEX_CONTRACT_ID_LIST") or os.getenv("MIRROR_CONTRACT_IDS") or cfg.get("contract_id_list")
    force_env = _env_bool("MIRROR_FORCE_ENV_SYMBOLS", False)
    auto_symbols = _env_bool("MIRROR_AUTO_SYMBOLS", True)
    symbols: List[str] = []
    if force_env and (sym_list_env or sym_single):
        if sym_list_env:
            symbols = [s.strip() for s in str(sym_list_env).split(",") if s.strip()]
        elif sym_single:
            symbols = [str(sym_single)]
    # それ以外は後段で自動検出（空のままにしておく）

    # loop settings
    try:
        poll_interval = float(os.getenv("MIRROR_POLL_SEC", cfg.get("mirror_poll_sec", 3.0)))
    except Exception:
        poll_interval = 3.0
    try:
        step_cap = float(os.getenv("MIRROR_STEP_MAX", cfg.get("mirror_step_max", 0.0)))
    except Exception:
        step_cap = 0.0
    try:
        notional_trigger_usd = float(os.getenv("MIRROR_NOTIONAL_TRIGGER_USD", cfg.get("mirror_notional_trigger_usd", 0.0)))
    except Exception:
        notional_trigger_usd = 0.0

    # sanity
    from urllib.parse import urlparse
    parsed = urlparse(base_url or "")
    if not parsed.scheme or not parsed.netloc:
        raise SystemExit("EDGEX_BASE_URL が不正です（https://ホスト名 を設定してください）")
    if parsed.hostname and "example" in parsed.hostname:
        raise SystemExit("EDGEX_BASE_URL がプレースホルダです。実際のAPIベースURLに置き換えてください。")

    if not sdk_key:
        raise SystemExit("EDGEX_STARK_PRIVATE_KEY (or EDGEX_L2_KEY) が未設定です")
    if not api_id:
        raise SystemExit("EDGEX_ACCOUNT_ID が未設定です")

    # GAS allow-list check (same behavior as grid bot)
    await auth_check_if_required(cfg, str(api_id))

    client = EdgeXClient(base_url=base_url, account_id=int(api_id), stark_private_key=str(sdk_key))

    # Reference source: require private key for reference account
    ref_key = os.getenv("REF_STARK_PRIVATE_KEY") or cfg.get("ref_stark_private_key")
    if not ref_key:
        raise SystemExit("REF_STARK_PRIVATE_KEY が未設定です（参照アカウントの秘密鍵が必要）")
    ref_client: EdgeXClient = EdgeXClient(base_url=base_url, account_id=int(ref_id_env), stark_private_key=str(ref_key))

    # 資金移動（リバランス）機能は無効化（要求により削除）

    # Autodiscover symbols if not provided, or when auto mode is enabled
    if auto_symbols:
        discovered: List[str] = await discover_contracts_from_ref(ref_client, int(ref_id_env))
        if discovered:
            # 自動検出を最優先（強制時は既存symbols優先だが、未設定なら検出結果）
            symbols = discovered if not force_env else (symbols or discovered)
        if not symbols:
            logger.info("参照アカウントに保有ポジションが見つかりませんでした（自動検出=0件）。待機します。")

    try:
        logger.info("mirror hedger boot: base_url={} account_id={} ref_account_id={} symbols={} poll={}s step_cap={} trigger_usd={} ref_mode=client",
                    base_url, api_id, ref_id_env, symbols, poll_interval, step_cap, notional_trigger_usd)
        while True:
            for cid in symbols:
                try:
                    await mirror_once(
                        client,
                        base_url,
                        int(ref_id_env),
                        int(api_id),
                        str(cid),
                        step_cap,
                        ref_client=ref_client,
                        notional_trigger_usd=notional_trigger_usd,
                    )
                except Exception as e:
                    logger.warning("mirror for cid={} failed: {}", cid, e)
            await asyncio.sleep(7.0)
    finally:
        try:
            await client.close()
        except Exception:
            pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("stopped by user")




