#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import html
import json
import math
import re
import sys
import urllib.parse
import urllib.request
from collections import deque
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Deque, Dict, Iterable, List, Optional, Sequence, Set, Tuple

WIKI_SP500_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?period1={start}&period2={end}&interval=1d&includePrePost=false&events=div%2Csplits"
INITIAL_CAPITAL = 100_000.0
COMMISSION_RATE = 0.001
SLIPPAGE_RATE = 0.0005
MAX_POSITION_WEIGHT = 0.20
MAX_HOLDINGS = 5
STOP_LOSS_PCT = 0.07
TAKE_PROFIT_PCT = 0.20
SHORT_WINDOW = 5
LONG_WINDOW = 20
VOLUME_WINDOW = 20
EARNINGS_BLACKOUT_DAYS = 3
DEFAULT_START = "2020-01-01"
DEFAULT_END = "2025-03-21"
USER_AGENT = "Mozilla/5.0 (compatible; CodexBacktest/1.0)"


@dataclass
class Bar:
    date: str
    close: float
    adj_close: float
    volume: float
    ma5: Optional[float] = None
    ma20: Optional[float] = None
    vol_avg20: Optional[float] = None
    golden_cross: bool = False
    death_cross: bool = False


@dataclass
class Position:
    symbol: str
    shares: int
    entry_date: str
    entry_fill_price: float
    entry_cost_with_fees: float


@dataclass
class Trade:
    symbol: str
    side: str
    date: str
    signal_price: float
    fill_price: float
    shares: int
    gross_amount: float
    fees: float
    net_amount: float
    reason: str
    pnl: Optional[float] = None
    pnl_pct: Optional[float] = None


def normalize_symbol(symbol: str) -> str:
    return symbol.replace('.', '-').strip().upper()


def date_to_epoch(date_text: str) -> int:
    dt = datetime.strptime(date_text, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def fetch_text(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=60) as response:
        return response.read().decode("utf-8", errors="ignore")


def fetch_json(url: str) -> dict:
    return json.loads(fetch_text(url))


def strip_tags(text: str) -> str:
    return html.unescape(re.sub(r"<[^>]+>", "", text)).strip()


def fetch_sp500_symbols() -> List[str]:
    page = fetch_text(WIKI_SP500_URL)
    table_match = re.search(r'<table[^>]*id="constituents"[^>]*>(.*?)</table>', page, re.S | re.I)
    if not table_match:
        raise RuntimeError("Unable to locate the S&P 500 constituents table on Wikipedia.")
    table_html = table_match.group(1)
    row_matches = re.findall(r"<tr>(.*?)</tr>", table_html, re.S | re.I)
    symbols: List[str] = []
    for row_html in row_matches[1:]:
        cells = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row_html, re.S | re.I)
        if not cells:
            continue
        symbol = strip_tags(cells[0])
        if symbol:
            symbols.append(normalize_symbol(symbol))
    return sorted(set(symbols))


def fetch_yahoo_history(symbol: str, start_date: str, end_date: str) -> List[Bar]:
    start_epoch = date_to_epoch(start_date)
    end_epoch = date_to_epoch(end_date) + 86400
    url = YAHOO_CHART_URL.format(symbol=urllib.parse.quote(symbol), start=start_epoch, end=end_epoch)
    payload = fetch_json(url)
    result = payload.get("chart", {}).get("result")
    if not result:
        return []
    result0 = result[0]
    timestamps = result0.get("timestamp") or []
    indicators = result0.get("indicators", {})
    quote = (indicators.get("quote") or [{}])[0]
    adjclose = (indicators.get("adjclose") or [{}])[0].get("adjclose") or []
    closes = quote.get("close") or []
    volumes = quote.get("volume") or []
    bars: List[Bar] = []
    for i, ts in enumerate(timestamps):
        close = closes[i] if i < len(closes) else None
        adj = adjclose[i] if i < len(adjclose) else close
        volume = volumes[i] if i < len(volumes) else None
        if close is None or adj is None or volume is None:
            continue
        date_text = datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d")
        bars.append(Bar(date=date_text, close=float(close), adj_close=float(adj), volume=float(volume)))
    return bars


def load_price_csv(path: Path) -> List[Bar]:
    bars: List[Bar] = []
    with path.open() as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            date = (row.get("date") or row.get("Date") or "").strip()
            close = row.get("close") or row.get("Close")
            adj_close = row.get("adj_close") or row.get("Adj Close") or close
            volume = row.get("volume") or row.get("Volume")
            if not date or close is None or adj_close is None or volume is None:
                continue
            bars.append(
                Bar(
                    date=date[:10],
                    close=float(close),
                    adj_close=float(adj_close),
                    volume=float(volume),
                )
            )
    return bars


def load_earnings_dates(symbol: str, earnings_dir: Optional[Path]) -> Set[str]:
    if earnings_dir is None:
        return set()
    earnings_file = earnings_dir / f"{symbol}.csv"
    if not earnings_file.exists():
        return set()
    blackout_dates: Set[str] = set()
    with earnings_file.open() as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            raw = (row.get("date") or row.get("earnings_date") or "").strip()
            if not raw:
                continue
            base = datetime.strptime(raw[:10], "%Y-%m-%d")
            for offset in range(-EARNINGS_BLACKOUT_DAYS, EARNINGS_BLACKOUT_DAYS + 1):
                blackout_dates.add((base + timedelta(days=offset)).strftime("%Y-%m-%d"))
    return blackout_dates


def enrich_bars(bars: List[Bar]) -> None:
    price_window: Deque[float] = deque(maxlen=LONG_WINDOW)
    vol_window: Deque[float] = deque(maxlen=VOLUME_WINDOW)
    prev_ma5: Optional[float] = None
    prev_ma20: Optional[float] = None
    for bar in bars:
        price_window.append(bar.adj_close)
        vol_window.append(bar.volume)
        if len(price_window) >= SHORT_WINDOW:
            bar.ma5 = sum(list(price_window)[-SHORT_WINDOW:]) / SHORT_WINDOW
        if len(price_window) >= LONG_WINDOW:
            bar.ma20 = sum(price_window) / LONG_WINDOW
        if len(vol_window) >= VOLUME_WINDOW:
            bar.vol_avg20 = sum(vol_window) / VOLUME_WINDOW
        if prev_ma5 is not None and prev_ma20 is not None and bar.ma5 is not None and bar.ma20 is not None:
            bar.golden_cross = bar.ma5 > bar.ma20 and prev_ma5 <= prev_ma20
            bar.death_cross = bar.ma5 < bar.ma20 and prev_ma5 >= prev_ma20
        prev_ma5 = bar.ma5
        prev_ma20 = bar.ma20


def build_bar_map(bars: Sequence[Bar]) -> Dict[str, Bar]:
    return {bar.date: bar for bar in bars}


def portfolio_equity(cash: float, positions: Dict[str, Position], current_prices: Dict[str, float], last_prices: Dict[str, float]) -> float:
    total = cash
    for symbol, position in positions.items():
        price = current_prices.get(symbol, last_prices.get(symbol))
        if price is not None:
            total += position.shares * price
    return total


def calc_max_drawdown(equity_points: Sequence[Tuple[str, float]]) -> float:
    running_max = -math.inf
    max_dd = 0.0
    for _, value in equity_points:
        running_max = max(running_max, value)
        if running_max > 0:
            max_dd = min(max_dd, value / running_max - 1.0)
    return max_dd


def date_span_years(start_date: str, end_date: str) -> float:
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    return max((end - start).days / 365.25, 0.0)


def write_csv(path: Path, rows: List[Dict[str, object]], fieldnames: List[str]) -> None:
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def generate_svg_chart(path: Path, equity_curve: List[Dict[str, object]], trades: List[Trade], spy_series: List[Tuple[str, float, Optional[float], bool]]) -> None:
    width, height = 1400, 900
    margin_left, margin_right, margin_top, margin_bottom = 80, 40, 60, 60
    gap = 70
    top_h = 430
    bottom_h = 250

    dates = [row["Date"] for row in equity_curve]
    if not dates:
        raise RuntimeError("No equity data available for chart generation.")

    def scale_x(index: int, total: int) -> float:
        usable_w = width - margin_left - margin_right
        return margin_left + (usable_w * index / max(total - 1, 1))

    def build_path(values: List[float], y_top: int, panel_h: int) -> str:
        vmin, vmax = min(values), max(values)
        spread = max(vmax - vmin, 1e-9)
        points = []
        for idx, value in enumerate(values):
            x = scale_x(idx, len(values))
            y = y_top + panel_h - ((value - vmin) / spread) * panel_h
            points.append(f"{x:.2f},{y:.2f}")
        return " ".join(points), vmin, vmax

    equity_values = [float(row["Equity"]) for row in equity_curve]
    equity_path, equity_min, equity_max = build_path(equity_values, margin_top, top_h)

    spy_values = [value for _, value, _, _ in spy_series]
    spy_path, spy_min, spy_max = build_path(spy_values, margin_top + top_h + gap, bottom_h)
    ma_values = [ma if ma is not None else value for _, value, ma, _ in spy_series]
    ma_path, _, _ = build_path(ma_values, margin_top + top_h + gap, bottom_h)

    date_to_equity_xy: Dict[str, Tuple[float, float]] = {}
    spread = max(equity_max - equity_min, 1e-9)
    for idx, row in enumerate(equity_curve):
        x = scale_x(idx, len(equity_curve))
        y = margin_top + top_h - ((float(row["Equity"]) - equity_min) / spread) * top_h
        date_to_equity_xy[str(row["Date"])] = (x, y)

    buy_markers = []
    sell_markers = []
    for trade in trades:
        point = date_to_equity_xy.get(trade.date)
        if not point:
            continue
        x, y = point
        if trade.side == "BUY":
            buy_markers.append(f'<polygon points="{x:.1f},{y-10:.1f} {x-7:.1f},{y+6:.1f} {x+7:.1f},{y+6:.1f}" fill="green" />')
        else:
            sell_markers.append(f'<polygon points="{x:.1f},{y+10:.1f} {x-7:.1f},{y-6:.1f} {x+7:.1f},{y-6:.1f}" fill="red" />')

    svg = f'''<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">
  <rect width="100%" height="100%" fill="white" />
  <text x="{width/2}" y="35" text-anchor="middle" font-size="24" font-family="Arial">均线趋势策略买卖点图</text>
  <text x="{margin_left}" y="55" font-size="14" font-family="Arial">上图：组合权益曲线与买卖点；下图：SPY 与 20 日均线过滤</text>
  <line x1="{margin_left}" y1="{margin_top + top_h}" x2="{width - margin_right}" y2="{margin_top + top_h}" stroke="#444" />
  <line x1="{margin_left}" y1="{margin_top}" x2="{margin_left}" y2="{margin_top + top_h}" stroke="#444" />
  <polyline fill="none" stroke="navy" stroke-width="2" points="{equity_path}" />
  {''.join(buy_markers)}
  {''.join(sell_markers)}
  <text x="{margin_left}" y="{margin_top - 10}" font-size="14" font-family="Arial">Equity max: {equity_max:,.2f}</text>
  <text x="{margin_left + 220}" y="{margin_top - 10}" font-size="14" font-family="Arial">Equity min: {equity_min:,.2f}</text>
  <line x1="{margin_left}" y1="{margin_top + top_h + gap + bottom_h}" x2="{width - margin_right}" y2="{margin_top + top_h + gap + bottom_h}" stroke="#444" />
  <line x1="{margin_left}" y1="{margin_top + top_h + gap}" x2="{margin_left}" y2="{margin_top + top_h + gap + bottom_h}" stroke="#444" />
  <polyline fill="none" stroke="black" stroke-width="2" points="{spy_path}" />
  <polyline fill="none" stroke="orange" stroke-width="2" points="{ma_path}" />
  <text x="{margin_left}" y="{margin_top + top_h + gap - 10}" font-size="14" font-family="Arial">SPY max: {spy_max:,.2f}</text>
  <text x="{margin_left + 220}" y="{margin_top + top_h + gap - 10}" font-size="14" font-family="Arial">SPY min: {spy_min:,.2f}</text>
  <text x="{width - 200}" y="{margin_top + 20}" font-size="14" font-family="Arial" fill="green">▲ BUY</text>
  <text x="{width - 120}" y="{margin_top + 20}" font-size="14" font-family="Arial" fill="red">▼ SELL</text>
  <text x="{width - 240}" y="{margin_top + top_h + gap + 20}" font-size="14" font-family="Arial" fill="black">SPY</text>
  <text x="{width - 180}" y="{margin_top + top_h + gap + 20}" font-size="14" font-family="Arial" fill="orange">MA20</text>
</svg>'''
    path.write_text(svg)


def run_backtest(symbol_bars: Dict[str, List[Bar]], spy_bars: List[Bar], earnings_dir: Optional[Path]) -> Tuple[List[Dict[str, object]], List[Trade], Dict[str, float], List[Tuple[str, float, Optional[float], bool]], List[str]]:
    spy_map = build_bar_map(spy_bars)
    dates = [bar.date for bar in spy_bars if bar.ma20 is not None]
    positions: Dict[str, Position] = {}
    trades: List[Trade] = []
    equity_curve: List[Dict[str, object]] = []
    warnings: List[str] = []
    cash = INITIAL_CAPITAL
    last_prices: Dict[str, float] = {}
    earnings_available = earnings_dir is not None and earnings_dir.exists()
    if not earnings_available:
        warnings.append("未提供财报日期目录，财报前后 3 天过滤未生效。")

    blackout_by_symbol = {symbol: load_earnings_dates(symbol, earnings_dir) for symbol in symbol_bars}
    bar_maps = {symbol: build_bar_map(bars) for symbol, bars in symbol_bars.items()}
    spy_series: List[Tuple[str, float, Optional[float], bool]] = []

    for date in dates:
        spy_bar = spy_map[date]
        can_open_market = spy_bar.ma20 is not None and spy_bar.adj_close > spy_bar.ma20
        spy_series.append((date, spy_bar.adj_close, spy_bar.ma20, can_open_market))
        current_prices: Dict[str, float] = {}

        # Exit first.
        for symbol in list(positions.keys()):
            bar = bar_maps[symbol].get(date)
            if bar is None:
                continue
            price = bar.adj_close
            current_prices[symbol] = price
            last_prices[symbol] = price
            position = positions[symbol]
            stop_price = position.entry_fill_price * (1 - STOP_LOSS_PCT)
            take_profit_price = position.entry_fill_price * (1 + TAKE_PROFIT_PCT)

            sell_reason: Optional[str] = None
            if price <= stop_price:
                sell_reason = "stop_loss"
            elif price >= take_profit_price:
                sell_reason = "take_profit"
            elif bar.death_cross:
                sell_reason = "death_cross"

            if sell_reason is None:
                continue

            fill_price = price * (1 - SLIPPAGE_RATE)
            gross_amount = position.shares * fill_price
            fees = gross_amount * COMMISSION_RATE
            net_amount = gross_amount - fees
            cash += net_amount
            pnl = net_amount - position.entry_cost_with_fees
            pnl_pct = pnl / position.entry_cost_with_fees if position.entry_cost_with_fees else None
            trades.append(Trade(symbol, "SELL", date, price, fill_price, position.shares, gross_amount, fees, net_amount, sell_reason, pnl, pnl_pct))
            del positions[symbol]

        current_equity = portfolio_equity(cash, positions, current_prices, last_prices)

        if can_open_market and len(positions) < MAX_HOLDINGS:
            candidates: List[Tuple[str, float]] = []
            for symbol, bars in symbol_bars.items():
                if symbol in positions:
                    continue
                bar = bar_maps[symbol].get(date)
                if bar is None or bar.ma20 is None or bar.vol_avg20 is None:
                    continue
                if not bar.golden_cross:
                    continue
                if bar.volume < bar.vol_avg20:
                    continue
                if date in blackout_by_symbol.get(symbol, set()):
                    continue
                candidates.append((symbol, bar.adj_close))
            candidates.sort(key=lambda item: item[0])
            per_position_budget = current_equity * MAX_POSITION_WEIGHT
            slots = MAX_HOLDINGS - len(positions)
            for symbol, signal_price in candidates[:slots]:
                fill_price = signal_price * (1 + SLIPPAGE_RATE)
                per_share_cost = fill_price * (1 + COMMISSION_RATE)
                shares = int(min(cash, per_position_budget) // per_share_cost)
                if shares <= 0:
                    continue
                gross_amount = shares * fill_price
                fees = gross_amount * COMMISSION_RATE
                total_cost = gross_amount + fees
                if total_cost > cash:
                    continue
                cash -= total_cost
                positions[symbol] = Position(symbol, shares, date, fill_price, total_cost)
                last_prices[symbol] = signal_price
                trades.append(Trade(symbol, "BUY", date, signal_price, fill_price, shares, gross_amount, fees, -total_cost, "golden_cross"))

        equity_curve.append({
            "Date": date,
            "Cash": round(cash, 6),
            "Positions": len(positions),
            "Equity": round(portfolio_equity(cash, positions, current_prices, last_prices), 6),
        })

    if positions and equity_curve:
        final_date = equity_curve[-1]["Date"]
        for symbol in list(positions.keys()):
            bar = bar_maps[symbol].get(final_date)
            if bar is None:
                continue
            position = positions[symbol]
            fill_price = bar.adj_close * (1 - SLIPPAGE_RATE)
            gross_amount = position.shares * fill_price
            fees = gross_amount * COMMISSION_RATE
            net_amount = gross_amount - fees
            cash += net_amount
            pnl = net_amount - position.entry_cost_with_fees
            pnl_pct = pnl / position.entry_cost_with_fees if position.entry_cost_with_fees else None
            trades.append(Trade(symbol, "SELL", final_date, bar.adj_close, fill_price, position.shares, gross_amount, fees, net_amount, "final_liquidation", pnl, pnl_pct))
            del positions[symbol]
        equity_curve[-1]["Cash"] = round(cash, 6)
        equity_curve[-1]["Positions"] = 0
        equity_curve[-1]["Equity"] = round(cash, 6)

    final_equity = float(equity_curve[-1]["Equity"]) if equity_curve else INITIAL_CAPITAL
    total_return = final_equity / INITIAL_CAPITAL - 1.0
    years = date_span_years(equity_curve[0]["Date"], equity_curve[-1]["Date"]) if equity_curve else 0.0
    annual_return = (final_equity / INITIAL_CAPITAL) ** (1 / years) - 1 if years > 0 else 0.0
    max_drawdown = calc_max_drawdown([(row["Date"], float(row["Equity"])) for row in equity_curve]) if equity_curve else 0.0
    metrics = {
        "total_return": total_return,
        "annual_return": annual_return,
        "max_drawdown": max_drawdown,
        "final_equity": final_equity,
        "trade_count": len(trades),
    }
    return equity_curve, trades, metrics, spy_series, warnings


def save_outputs(output_dir: Path, equity_curve: List[Dict[str, object]], trades: List[Trade], metrics: Dict[str, float], spy_series: List[Tuple[str, float, Optional[float], bool]], warnings: List[str]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    write_csv(output_dir / "equity_curve.csv", equity_curve, ["Date", "Cash", "Positions", "Equity"])
    write_csv(output_dir / "trades.csv", [asdict(trade) for trade in trades], [
        "symbol", "side", "date", "signal_price", "fill_price", "shares", "gross_amount", "fees", "net_amount", "reason", "pnl", "pnl_pct"
    ])
    (output_dir / "metrics.json").write_text(json.dumps(metrics, indent=2, ensure_ascii=False))
    generate_svg_chart(output_dir / "buy_sell_points.svg", equity_curve, trades, spy_series)

    report_lines = [
        "# 均线趋势策略回测报告",
        "",
        f"- 总收益率：{metrics['total_return']:.2%}",
        f"- 年化收益率：{metrics['annual_return']:.2%}",
        f"- 最大回撤：{metrics['max_drawdown']:.2%}",
        f"- 期末权益：${metrics['final_equity']:,.2f}",
        f"- 交易笔数：{metrics['trade_count']}",
        "",
        "## 输出文件",
        "",
        "- `equity_curve.csv`",
        "- `trades.csv`",
        "- `metrics.json`",
        "- `buy_sell_points.svg`",
    ]
    if warnings:
        report_lines.extend(["", "## 注意事项", ""])
        report_lines.extend([f"- {item}" for item in warnings])
    (output_dir / "report.md").write_text("\n".join(report_lines) + "\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backtest the requested S&P 500 moving-average strategy.")
    parser.add_argument("--start", default=DEFAULT_START)
    parser.add_argument("--end", default=DEFAULT_END)
    parser.add_argument("--output-dir", default="output")
    parser.add_argument("--earnings-dir", default=None, help="Optional directory containing per-symbol earnings CSV files with a 'date' column.")
    parser.add_argument("--symbols-file", default=None, help="Optional local file with one symbol per line.")
    parser.add_argument("--prices-dir", default=None, help="Optional directory containing per-symbol OHLCV CSV files for offline backtests.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        all_bars: Dict[str, List[Bar]] = {}
        if args.symbols_file and args.prices_dir:
            symbols = [normalize_symbol(line.strip()) for line in Path(args.symbols_file).read_text().splitlines() if line.strip()]
            if "SPY" not in symbols:
                symbols.append("SPY")
            prices_dir = Path(args.prices_dir)
            for symbol in symbols:
                csv_path = prices_dir / f"{symbol}.csv"
                if not csv_path.exists():
                    continue
                bars = load_price_csv(csv_path)
                if bars:
                    enrich_bars(bars)
                    all_bars[symbol] = bars
        else:
            symbols = fetch_sp500_symbols()
            if "SPY" not in symbols:
                symbols.append("SPY")
            for symbol in symbols:
                bars = fetch_yahoo_history(symbol, args.start, args.end)
                if bars:
                    enrich_bars(bars)
                    all_bars[symbol] = bars
        if "SPY" not in all_bars:
            raise RuntimeError("Unable to fetch SPY data required for the market filter.")
        spy_bars = all_bars.pop("SPY")
        if not all_bars:
            raise RuntimeError("Unable to fetch constituent price history.")
        earnings_dir = Path(args.earnings_dir) if args.earnings_dir else None
        equity_curve, trades, metrics, spy_series, warnings = run_backtest(all_bars, spy_bars, earnings_dir)
        save_outputs(Path(args.output_dir), equity_curve, trades, metrics, spy_series, warnings)
        print(json.dumps(metrics, indent=2, ensure_ascii=False))
        if warnings:
            print("Warnings:")
            for item in warnings:
                print(f"- {item}")
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise


if __name__ == "__main__":
    main()
