# triangular-arbitrage-scanner

A multi-exchange triangular arbitrage paper-trading scanner for crypto markets. Connects to live WebSocket order book data across multiple exchanges (Binance, Kraken), auto-generates valid three-leg trading triangles per exchange based on real market metadata, and simulates fee- and depth-adjusted profitability with a latency-survival check. Detection and logging only — no live order placement.

## How it works
- Connects via WebSocket (not polling) to each exchange's public order book feed
- Auto-generates candidate triangles by walking the exchange's own market list, ranked by liquidity (lowest-volume leg as the bottleneck score)
- Walks real order book depth (not just top-of-book price) to calculate a realistic fill price for a given trade size
- Applies exchange-specific taker fees across all three legs
- On detecting a profitable opportunity, simulates a delay (representing real network/execution latency) and re-checks whether the opportunity still holds
- Logs every detected opportunity to CSV with theoretical vs. post-latency profit, for later analysis

## Running it
Deployed as a systemd service on a small Ubuntu VPS for 24/7 uptime. Requires Python 3 and the `ccxt` library.

## Problems hit and fixed along the way
- **Rate limiting**: opening too many WebSocket subscriptions at once on startup caused exchanges to reject connections. Fixed by staggering subscription requests and capping the number of tracked triangles/symbols per exchange.
- **Memory crashes**: running on a 512MB VPS caused the process to be killed (OOM) once multiple exchanges were connected simultaneously. Fixed by resizing to a 1GB instance and confirming stable memory usage under load.
- **Stale/skewed data false positives**: added checks to reject any "opportunity" where the three legs' price data wasn't fresh or wasn't captured close enough together in time, to avoid logging fake signals from mismatched data.
- **Fee accuracy**: different exchanges charge different taker fees — the scanner tracks these per-exchange rather than assuming one flat rate, which materially changes which opportunities are actually real.

## Status
Currently running as a live paper-trading experiment to determine whether real, tradeable triangular arbitrage opportunities exist on these exchanges after accounting for fees, depth, and latency — before considering any live execution.
