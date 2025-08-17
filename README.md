## Momentum Investing Strategy

### Introduction
This repository implements a momentum investment strategy focused on stocks within the NSE (National Stock Exchange) 500 index, with occasional references to the NSE 100 and NSE 200 indices. The strategy aims to capitalize on the momentum effect observed in financial markets, where stocks that have performed well in the recent past tend to continue performing well in the near future.

### Results
- **Performance**: Over a 9-year backtest period, the **NSE 500 index delivered an approximate CAGR of 18%**, while this strategy achieved a **CAGR of ~30%** — significantly outperforming the benchmark.
- **Sharpe Ratio**: Although the strategy outperforms in terms of absolute returns, the **Sharpe ratio remains just below 1**, indicating room for improvement in terms of risk-adjusted performance.
- **Strategy Insights**: Post-analysis explored dynamically adjusting the strategy based on **previous month’s returns**. In particular, during periods like **Covid (2020)**, switching to a **reverse momentum approach** (buying losers) led to a **4x increase in NAV**.
- **Conclusion**: While the base momentum framework shows strong results, future improvements could include regime detection or volatility filtering to enhance consistency and Sharpe ratio.

### Strategy Overview
The strategy involves the following key components:
- **Stock Selection**: Stocks are selected based on their performance over a specified lookback period. Specifically, stocks that have exhibited the highest returns over this period are chosen.
- **Equal Weighting**: Once selected, stocks are equally weighted in the portfolio.
- **Holding Period**: The selected stocks are held for a fixed holding period before being reassessed.
- **Rolling Basis**: The strategy operates on a rolling basis, meaning that stock selection and portfolio rebalancing occur periodically, typically at regular intervals.

### Stock Universe
The strategy focuses on stocks listed in the NSE 500 index. At times, references are made to the NSE 100 and NSE 200 indices, depending on data availability and research requirements.

### Time Period
The analysis and implementation of the strategy are based on historical data spanning from 2013 to 2022. Data completeness varies across time and stocks, and efforts have been made to ensure the highest possible data integrity.

### Data Considerations
Given the complexity of financial data and its management, this repository utilizes the best available measures to handle missing data and ensure robustness in the strategy's backtesting and implementation.

### Repository Structure
- **Code Implementation**: The repository includes scripts and notebooks that implement the momentum strategy, including data preprocessing, strategy backtesting, and performance evaluation.
- **Documentation**: Apart from this README, you can find the sheets used to recieve the data we use in this strategy in the Data_Sheets folder.

### Technical Knowhow

- **Parallel Processing**: This code runs multiple date-wise backtest computations in parallel using `ThreadPoolExecutor`, improving overall runtime by approximately **30%**.
- **Unified Configuration**: The strategy is implemented in a single, adaptable codebase. You can easily configure parameters like **holding period (weekly/monthly)**, **lookback duration**, **number of portfolio stocks**, and **buy/sell date logic** — all without needing to maintain separate versions of the code.
