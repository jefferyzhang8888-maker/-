# S&P 500 均线趋势策略回测

这个仓库提供一个**纯 Python 标准库**实现的回测脚本，用于尽量复现以下规则：

- 市场：美股
- 标的：S&P 500 成分股
- 周期：日线
- 买入：5 日均线上穿 20 日均线
- 卖出：5 日均线下穿 20 日均线
- 仓位：单只股票最多 20%，最多持有 5 只
- 风控：单笔亏损 7% 止损、盈利 20% 止盈
- 过滤：
  - 仅在 SPY 收盘位于 20 日均线上方时允许开仓
  - 当日成交量低于 20 日平均成交量时不买
  - 可选：财报日前后 3 天不买（需要额外提供本地财报 CSV）
- 成本：手续费 0.1%，滑点 0.05%
- 初始资金：100,000 美元

## 使用方法

### 1. 直接运行

```bash
python backtest_sp500_ma_strategy.py \
  --start 2020-01-01 \
  --end 2025-03-21 \
  --output-dir output
```

### 2. 如果你有财报日期数据

将财报文件放到一个目录中，按股票代码命名，例如：

- `earnings/AAPL.csv`
- `earnings/MSFT.csv`

每个 CSV 至少包含一列：

```csv
date
2024-01-30
2024-04-25
```

然后运行：

```bash
python backtest_sp500_ma_strategy.py \
  --start 2020-01-01 \
  --end 2025-03-21 \
  --earnings-dir earnings \
  --output-dir output
```

### 3. 离线回测模式

如果当前环境无法访问 Wikipedia / Yahoo Finance，可以提供本地股票列表和价格 CSV：

```bash
python backtest_sp500_ma_strategy.py \
  --symbols-file tests/fixtures/symbols.txt \
  --prices-dir tests/fixtures/prices \
  --output-dir output_local
```

每个价格 CSV 需要至少包含以下列：

```csv
date,close,adj_close,volume
2020-01-01,100,100,1200000
```

## 输出结果

脚本会生成：

- `output/metrics.json`：总收益率、年化收益率、最大回撤、期末权益、交易笔数
- `output/equity_curve.csv`：组合权益曲线
- `output/trades.csv`：逐笔交易明细
- `output/buy_sell_points.svg`：买卖点图（SVG）
- `output/report.md`：摘要报告

## 说明

- S&P 500 成分股列表通过 Wikipedia 页面抓取，默认使用**当前成分股**，因此存在存续偏差（survivorship bias）。
- 价格数据通过 Yahoo Finance 图表接口抓取。
- 由于财报历史日期很难稳定地从免密公开接口完整获取，因此脚本把财报过滤设计为**可选的本地输入**。未提供时，报告里会明确标注该限制。
