"""Paper-only, multi-exchange triangular scanner.

No credentials or private/trading methods are used.  ``load_markets`` is a
one-time public metadata request; all book prices use ccxt.pro WebSockets.
"""
import asyncio, csv, logging, math, random, time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
import ccxt.pro as ccxtpro

# Conservative test-run defaults. Add coinbase/okx/bybit here only after their
# public WebSocket behaviour has been verified independently.
EXCHANGE_IDS=["binance","kraken"]
# Assumed/configurable paper fees; these are not authenticated account fees.
# Must exactly match EXCHANGE_IDS. Add the matching assumed paper fee here when
# re-enabling another exchange.
TAKER_FEES={"binance":.001,"kraken":.004}
# Triangles are generated independently from each exchange's public spot-market
# metadata.  Start currency controls which closed three-leg routes are scanned.
START_CURRENCY="USDT"
MAX_TRIANGLES_PER_EXCHANGE=5 # conservative test-run cap; configurable
MAX_UNIQUE_SYMBOLS_PER_EXCHANGE=20 # hard cap on public book streams
WS_SUBSCRIPTION_STAGGER_SECONDS=.25 # spacing between initial unique symbols
MAX_CONCURRENT_SUBSCRIPTIONS=1 # stability is preferred to startup speed
RECONNECT_JITTER_SECONDS=.5 # random 0..this added to exponential backoff
BOOK_READINESS_TIMEOUT_SECONDS=45.0
ASSUMED_ORDER_SIZE=1000.0 # input USDT
MIN_PROFIT_THRESHOLD=.001 # post-fee, depth-walk and risk-buffer fraction
ASSUMED_LATENCY_SECONDS=.2
EXTRA_SLIPPAGE_BUFFER=.0005 # independent execution-risk haircut; not price impact
MAX_BOOK_LEVELS=None
MIN_DEPTH_UTILISATION=.98 # fraction of each requested leg input that must be executable
MAX_BOOK_AGE_SECONDS=2.0
MAX_TRIANGLE_BOOK_SKEW_SECONDS=.5
MAX_PENDING_LATENCY_TASKS=20
MIN_LOG_INTERVAL_SECONDS=1.0
INITIAL_RECONNECT_DELAY_SECONDS=2.0
MAX_RECONNECT_DELAY_SECONDS=60.0
STATUS_REPORT_INTERVAL_SECONDS=30.0
LOG_FILE=Path("triangular_arbitrage_opportunities.csv")

logging.basicConfig(level=logging.INFO,format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger=logging.getLogger("triangular-arbitrage")
CSV_FIELDS=["timestamp","exchange","triangle","input_amount","fee_assumption","top_of_book_profit_pct","depth_adjusted_profit_pct","risk_adjusted_profit_pct","post_latency_profit_pct","profit_amount","latency_ms","book_age_leg1","book_age_leg2","book_age_leg3","triangle_book_skew_seconds","latency_survival","failure_reason","detected_final_amount","post_latency_final_amount","leg_fill_prices","leg_top_prices","levels_consumed","available_depth","fillability","price_impacts","total_price_impact","extra_slippage_buffer"]

@dataclass
class BookState:
 symbol:str; book:dict; received_monotonic:float; exchange_timestamp:Optional[float]; nonce:Any; connected:bool=True; updates:int=1
@dataclass
class LegResult:
 pair:str; action:str; input_amount:float; output_before_fee:float; output_after_fee:float; fee_paid:float; top_price:float; average_price:float; price_impact:float; levels_consumed:int; executable_input_available:float; input_consumed:float; fully_fillable:bool
@dataclass
class SimulationResult:
 input_amount:float; top_of_book_final_amount:float; depth_adjusted_final_amount:float; final_amount:float; top_of_book_profit_pct:float; depth_adjusted_profit_pct:float; risk_adjusted_profit_pct:float; total_price_impact:float; extra_slippage_buffer:float; legs:list

def triangle_name(triangle): return " -> ".join([triangle[0]["from_currency"]]+[x["to_currency"] for x in triangle])
def exchange_logger(x): return logging.getLogger("triangular-arbitrage."+x)

def generate_triangles(markets, start_currency=START_CURRENCY):
 """Return every distinct executable three-leg spot cycle starting/ending here.

 Each market creates two directed public-book edges: quote->base is a buy and
 base->quote is a sell.  This is exchange-agnostic and deliberately excludes
 inactive, contract, and duplicate-pair loops.
 """
 edges={}
 for symbol,market in markets.items():
  if market.get("active") is False or market.get("spot") is False:continue
  base,quote=market.get("base"),market.get("quote")
  if not base or not quote or base==quote:continue
  edges.setdefault(quote,[]).append({"pair":symbol,"action":"buy","from_currency":quote,"to_currency":base})
  edges.setdefault(base,[]).append({"pair":symbol,"action":"sell","from_currency":base,"to_currency":quote})
 result=[];seen=set()
 for first in edges.get(start_currency,[]):
  for second in edges.get(first["to_currency"],[]):
   if second["pair"]==first["pair"]:continue
   for third in edges.get(second["to_currency"],[]):
    route=[first,second,third]
    if third["to_currency"]!=start_currency or third["pair"] in {first["pair"],second["pair"]}:continue
    key=tuple((x["pair"],x["action"]) for x in route)
    if key not in seen:seen.add(key);result.append(route)
 return result

def market_quote_volume(market):
 """Best available metadata-only 24h quote volume, never REST-polled.

 ccxt market metadata often does not include rolling volume. Missing values are
 scored as zero and logged at startup; depth checks remain the execution gate.
 """
 for source in (market,market.get("info",{})):
  for key in ("quoteVolume","quote_volume","volumeQuote","volCcy24h"):
   try:
    value=float(source.get(key))
    if value>=0 and math.isfinite(value):return value
   except (TypeError,ValueError):pass
 return 0.0

def select_triangles(markets, log):
 """Rank by minimum leg quote-volume (the bottleneck), then deterministic name.

 It first limits to MAX_TRIANGLES_PER_EXCHANGE, then admits a route only if its
 new unique symbols fit MAX_UNIQUE_SYMBOLS_PER_EXCHANGE.  Thus no cap is ever
 silently exceeded and shared symbols are subscribed only once.
 """
 candidates=[]
 for triangle in generate_triangles(markets):
  scores=[market_quote_volume(markets[x["pair"]]) for x in triangle]
  candidates.append((min(scores),triangle))
 candidates.sort(key=lambda item:(-item[0],triangle_name(item[1])))
 ranked=candidates[:MAX_TRIANGLES_PER_EXCHANGE]
 selected=[];symbols=set();rejected_symbols=0
 for score,triangle in ranked:
  required={x["pair"] for x in triangle}
  if len(symbols|required)>MAX_UNIQUE_SYMBOLS_PER_EXCHANGE:
   rejected_symbols+=1;continue
  selected.append((score,triangle));symbols.update(required)
 missing_volume=sum(1 for score,_ in candidates if score==0)
 if missing_volume:log.warning("%d candidate triangles lack metadata 24h quote volume; score=0",missing_volume)
 return selected,symbols,{"candidates":len(candidates),"triangle_cap_rejected":max(0,len(candidates)-len(ranked)),"symbol_cap_rejected":rejected_symbols}

class CsvLogger:
 def __init__(self,path=LOG_FILE): self.path,self.lock=path,asyncio.Lock()
 async def initialise(self):
  async with self.lock:
   if not self.path.exists():
    with self.path.open("w",newline="",encoding="utf-8") as f: csv.DictWriter(f,fieldnames=CSV_FIELDS).writeheader()
 async def write(self,row):
  async with self.lock:
   with self.path.open("a",newline="",encoding="utf-8") as f: csv.DictWriter(f,fieldnames=CSV_FIELDS).writerow(row)

def walk_order_book(book,action,amount_in):
 """Walk depth; returns actual depth impact, never fees or arbitrary buffer."""
 if not isinstance(book,dict) or not isinstance(amount_in,(int,float)) or amount_in<=0 or not math.isfinite(amount_in): return None
 if action not in ("buy","sell"): raise ValueError("Unknown action: "+str(action))
 levels=book.get("asks" if action=="buy" else "bids")
 if not isinstance(levels,list) or not levels: return None
 valid=[]
 for row in levels[:MAX_BOOK_LEVELS] if MAX_BOOK_LEVELS else levels:
  if not isinstance(row,(list,tuple)) or len(row)<2: continue
  try: p,q=float(row[0]),float(row[1])
  except (TypeError,ValueError): continue
  if p>0 and q>0 and math.isfinite(p) and math.isfinite(q): valid.append((p,q))
 if not valid:return None
 top=valid[0][0]; available=sum(p*q for p,q in valid) if action=="buy" else sum(q for _,q in valid)
 remaining=amount_in; output=consumed=notional=0.; used=0
 for price,qty in valid:
  base=min(qty,remaining/price) if action=="buy" else min(qty,remaining)
  if base<=0:continue
  quote=base*price; output+=base if action=="buy" else quote; consumed+=quote if action=="buy" else base; notional+=quote; remaining-=quote if action=="buy" else base; used+=1
  if remaining<=max(1e-12,amount_in*1e-9):break
 if not consumed:return None
 average=notional/output if action=="buy" else notional/consumed
 impact=average/top-1 if action=="buy" else top/average-1
 return {"amount_out":output,"average_price":average,"top_price":top,"price_impact":max(0.,impact),"levels_consumed":used,"executable_input_available":available,"input_consumed":consumed,"remaining_input":remaining,"fully_fillable":remaining<=max(1e-12,amount_in*1e-9)}

def books_are_coherent(states,triangle,now=None):
 now=time.monotonic() if now is None else now; found=[]
 for leg in triangle:
  state=states.get(leg["pair"])
  if state is None or not state.connected or state.updates<1:return False,{},None,"missing_or_unready_book"
  found.append(state)
 ages={x.symbol:max(0.,now-x.received_monotonic) for x in found}
 if any(x>MAX_BOOK_AGE_SECONDS for x in ages.values()):return False,ages,None,"stale_book"
 skew=max(x.received_monotonic for x in found)-min(x.received_monotonic for x in found)
 if skew>MAX_TRIANGLE_BOOK_SKEW_SECONDS:return False,ages,skew,"book_skew"
 return True,ages,skew,None

def simulate_triangle(states,triangle,starting_amount,fee):
 current=starting_amount; top_current=starting_amount; legs=[]; impacts=0.
 for spec in triangle:
  walked=walk_order_book(states[spec["pair"]].book,spec["action"],current)
  # MIN_DEPTH_UTILISATION is an explicit required input-fill fraction, and partial legs are rejected.
  if walked is None or not walked["fully_fillable"] or walked["input_consumed"]/current<MIN_DEPTH_UTILISATION:return None
  # The top-of-book stage is deliberately separate from depth walking.
  top_output=top_current/walked["top_price"] if spec["action"]=="buy" else top_current*walked["top_price"]
  top_current=top_output*(1-fee)
  after=walked["amount_out"]*(1-fee); impacts+=walked["price_impact"]
  legs.append(LegResult(spec["pair"],spec["action"],current,walked["amount_out"],after,walked["amount_out"]-after,walked["top_price"],walked["average_price"],walked["price_impact"],walked["levels_consumed"],walked["executable_input_available"],walked["input_consumed"],walked["fully_fillable"]))
  current=after
 buffered=current*(1-EXTRA_SLIPPAGE_BUFFER)
 return SimulationResult(starting_amount,top_current,current,buffered,top_current/starting_amount-1,current/starting_amount-1,buffered/starting_amount-1,impacts,EXTRA_SLIPPAGE_BUFFER,legs)

def signature(exchange,states,triangle,result):
 # New book update IDs/local times or meaningful profit changes make a new event.
 return (exchange,triangle_name(triangle),tuple((states[x["pair"]].nonce,states[x["pair"]].received_monotonic) for x in triangle),round(result.risk_adjusted_profit_pct,6))
def make_row(exchange,triangle,detected,after,ages,skew,reason):
 legs=detected.legs
 age_values=[ages.get(x["pair"]) for x in triangle]
 return {"timestamp":datetime.now(timezone.utc).isoformat(),"exchange":exchange,"triangle":triangle_name(triangle),"input_amount":detected.input_amount,"fee_assumption":TAKER_FEES[exchange],"top_of_book_profit_pct":detected.top_of_book_profit_pct*100,"depth_adjusted_profit_pct":detected.depth_adjusted_profit_pct*100,"risk_adjusted_profit_pct":detected.risk_adjusted_profit_pct*100,"post_latency_profit_pct":None if after is None else after.risk_adjusted_profit_pct*100,"profit_amount":detected.final_amount-detected.input_amount,"latency_ms":ASSUMED_LATENCY_SECONDS*1000,"book_age_leg1":age_values[0],"book_age_leg2":age_values[1],"book_age_leg3":age_values[2],"triangle_book_skew_seconds":skew,"latency_survival":"survived_profitably" if after and after.risk_adjusted_profit_pct>=MIN_PROFIT_THRESHOLD else "failed","failure_reason":reason,"detected_final_amount":detected.final_amount,"post_latency_final_amount":None if after is None else after.final_amount,"leg_fill_prices":"|".join(map(lambda x:str(x.average_price),legs)),"leg_top_prices":"|".join(map(lambda x:str(x.top_price),legs)),"levels_consumed":"|".join(map(lambda x:str(x.levels_consumed),legs)),"available_depth":"|".join(map(lambda x:str(x.executable_input_available),legs)),"fillability":"|".join(map(lambda x:str(x.fully_fillable),legs)),"price_impacts":"|".join(map(lambda x:str(x.price_impact),legs)),"total_price_impact":detected.total_price_impact,"extra_slippage_buffer":EXTRA_SLIPPAGE_BUFFER}

async def latency_check(exchange,states,triangle,detected,csv_log,log):
 await asyncio.sleep(ASSUMED_LATENCY_SECONDS) # detector deliberately does not await this task
 okay,ages,skew,reason=books_are_coherent(states,triangle)
 after=simulate_triangle(states,triangle,ASSUMED_ORDER_SIZE,TAKER_FEES[exchange]) if okay else None
 if after is None and reason is None:reason="not_executable_or_depth_insufficient"
 if after and after.risk_adjusted_profit_pct<MIN_PROFIT_THRESHOLD:reason="profit_disappeared";after=None
 log.info("Latency result: %s (%s)","survived" if after else "failed",reason or f"{after.risk_adjusted_profit_pct*100:.4f}%")
 await csv_log.write(make_row(exchange,triangle,detected,after,ages,skew,reason))

def report_latency_task(task,log):
 """Background failures are observable and cannot stop the detector."""
 if task.cancelled():return
 try:task.result()
 except Exception:log.exception("Latency simulation task failed",exc_info=True)

async def detection_loop(exchange,states,triangle,log,status,csv_log):
 pending=set(); last_sig=None; last_logged=0.
 try:
  while True:
   await asyncio.sleep(.01); pending={x for x in pending if not x.done()}
   okay,ages,skew,_=books_are_coherent(states,triangle)
   if not okay:continue
   result=simulate_triangle(states,triangle,ASSUMED_ORDER_SIZE,TAKER_FEES[exchange])
   if result is None or result.risk_adjusted_profit_pct<MIN_PROFIT_THRESHOLD:continue
   now=time.monotonic(); sig=signature(exchange,states,triangle,result)
   if sig==last_sig and now-last_logged<MIN_LOG_INTERVAL_SECONDS:continue
   if len(pending)>=MAX_PENDING_LATENCY_TASKS:log.warning("Latency task limit reached; opportunity skipped");continue
   last_sig,last_logged=sig,now;status["opportunities_found"]+=1
   log.info("DETECTED %s %.4f%% fee=%.4f%% ages=%s skew=%.3fs",triangle_name(triangle),result.risk_adjusted_profit_pct*100,TAKER_FEES[exchange]*100,ages,skew)
   task=asyncio.create_task(latency_check(exchange,states,triangle,result,csv_log,log));pending.add(task);task.add_done_callback(lambda t:report_latency_task(t,log))
 finally:
  for x in pending:x.cancel()
  await asyncio.gather(*pending,return_exceptions=True)

def classify_stream_error(error):
 text=str(error).lower()
 if "rate" in text and "limit" in text:return "rate_limit"
 if any(x in text for x in ("symbol","market","not found","invalid")):return "subscription"
 if any(x in text for x in ("websocket","connection","network","timeout","timed out")):return "network"
 return "exchange"

async def watch_symbol(exchange,symbol,states,log,status,subscription_gate):
 delay=INITIAL_RECONNECT_DELAY_SECONDS
 while True:
  try:
   # Gate both initial subscriptions and re-subscriptions. A single ccxt.pro
   # instance remains shared; this only controls simultaneous setup pressure.
   async with subscription_gate:
    book=await asyncio.wait_for(exchange.watch_order_book(symbol),timeout=30)
   if not isinstance(book,dict) or not book.get("asks") or not book.get("bids"):raise ValueError("malformed/incomplete book")
   old=states.get(symbol);states[symbol]=BookState(symbol,book,time.monotonic(),book.get("timestamp"),book.get("nonce"),True,(old.updates if old else 0)+1);status.setdefault("retrying",set()).discard(symbol);delay=INITIAL_RECONNECT_DELAY_SECONDS
  except asyncio.CancelledError:raise
  except Exception as error:
   if symbol in states:states[symbol].connected=False # pre-disconnect book is invalidated
   kind=classify_stream_error(error);status["state"]="reconnecting";status.setdefault("retrying",set()).add(symbol)
   jitter=random.uniform(0,RECONNECT_JITTER_SECONDS);retry=delay+jitter
   log.warning("%s %s error: %s; retry in %.2fs (backoff %.2f + jitter %.2f)",symbol,kind,error,retry,delay,jitter)
   await asyncio.sleep(retry);delay=min(delay*2,MAX_RECONNECT_DELAY_SECONDS)

async def wait_for_books(states,symbols,status,log):
 """Do not create detectors until every selected stream has a fresh update."""
 deadline=time.monotonic()+BOOK_READINESS_TIMEOUT_SECONDS
 while time.monotonic()<deadline:
  healthy=[s for s in symbols if s in states and states[s].connected and states[s].updates>0 and time.monotonic()-states[s].received_monotonic<=MAX_BOOK_AGE_SECONDS]
  status["healthy_symbols"]=len(healthy);status["stale_symbols"]=len(symbols)-len(healthy)
  if len(healthy)==len(symbols):status["state"]="ready";log.info("Exchange ready: %d/%d symbols have fresh updates",len(healthy),len(symbols));return True
  await asyncio.sleep(.1)
 log.warning("Exchange not ready after %.0fs: healthy=%d requested=%d",BOOK_READINESS_TIMEOUT_SECONDS,status.get("healthy_symbols",0),len(symbols));return False

def validate_market(exchange,market,log):
 if market.get("active") is False or market.get("spot") is False:raise ValueError(f"{exchange}: inactive/non-spot {market.get('symbol')}")
 for rule in ("amount","cost","price"):
  if not market.get("limits",{}).get(rule):log.warning("%s has no %s limits metadata",market.get("symbol"),rule)
 if any(market.get("precision",{}).get(k) is None for k in ("amount","price")):log.warning("%s has incomplete precision metadata",market.get("symbol"))

async def run_exchange_scanner(exchange,status,csv_log):
 log=exchange_logger(exchange);delay=INITIAL_RECONNECT_DELAY_SECONDS
 while True:
  client=None;tasks=[]
  try:
   klass=getattr(ccxtpro,exchange,None)
   if klass is None:status["state"]="given_up";log.error("Unknown ccxt.pro exchange");return
   client=klass({"enableRateLimit":True});status["state"]="loading_markets";await client.load_markets() # one metadata call, not book polling
   selected,symbols,counts=select_triangles(client.markets,log)
   if not selected:
    status["state"]="given_up";log.warning("No active three-leg spot triangles starting in %s",START_CURRENCY);return
   triangles=[triangle for _,triangle in selected];symbols=sorted(symbols)
   for symbol in symbols:validate_market(exchange,client.markets[symbol],log)
   log.info("EXCHANGE %s | markets=%d candidates=%d selected=%d triangle_cap_rejected=%d symbols=%d symbol_cap_rejected=%d",exchange.upper(),len(client.markets),counts["candidates"],len(triangles),counts["triangle_cap_rejected"],len(symbols),counts["symbol_cap_rejected"])
   for score,triangle in selected:log.info("Selected %s bottleneck_quote_volume=%.8f",triangle_name(triangle),score)
   states={};subscription_gate=asyncio.Semaphore(MAX_CONCURRENT_SUBSCRIPTIONS)
   # Deliberately sequential scheduling: task N is not even created until its
   # stagger slot, avoiding a simultaneous wake-up fan-out.
   for index,symbol in enumerate(symbols,1):
    log.info("[%d/%d] subscribing %s",index,len(symbols),symbol)
    tasks.append(asyncio.create_task(watch_symbol(client,symbol,states,log,status,subscription_gate)))
    if index<len(symbols):await asyncio.sleep(WS_SUBSCRIPTION_STAGGER_SECONDS)
   status["state"]="waiting_for_books"
   if not await wait_for_books(states,symbols,status,log):
    # Keep healthy watcher tasks alive; retry readiness after the next cycle.
    await asyncio.sleep(INITIAL_RECONNECT_DELAY_SECONDS)
    continue
   tasks.extend(asyncio.create_task(detection_loop(exchange,states,triangle,log,status,csv_log)) for triangle in triangles)
   await asyncio.gather(*tasks);delay=INITIAL_RECONNECT_DELAY_SECONDS
  except asyncio.CancelledError:raise
  except Exception as error:
   status["state"]="reconnecting";log.exception("scanner failure: %s; retry %.1fs",error,delay);await asyncio.sleep(delay);delay=min(delay*2,MAX_RECONNECT_DELAY_SECONDS)
  finally:
   for x in tasks:x.cancel()
   if tasks:await asyncio.gather(*tasks,return_exceptions=True)
   if client:
    try:await client.close()
    except Exception:pass

def validate_configuration():
 if not EXCHANGE_IDS or len(set(EXCHANGE_IDS))!=len(EXCHANGE_IDS):raise ValueError("exchange IDs must be unique/nonempty")
 if set(EXCHANGE_IDS)!=set(TAKER_FEES):raise ValueError("TAKER_FEES must exactly cover EXCHANGE_IDS")
 if any(not 0<=x<=.05 for x in TAKER_FEES.values()):raise ValueError("fees must be 0..5%")
 if not isinstance(START_CURRENCY,str) or not START_CURRENCY:raise ValueError("invalid start currency")
 if ASSUMED_ORDER_SIZE<=0 or ASSUMED_LATENCY_SECONDS<0 or MIN_PROFIT_THRESHOLD<0:raise ValueError("invalid size/latency/threshold")
 if not 0<MIN_DEPTH_UTILISATION<=1 or not 0<=EXTRA_SLIPPAGE_BUFFER<1:raise ValueError("invalid depth/buffer")
 if MAX_BOOK_AGE_SECONDS<=0 or MAX_TRIANGLE_BOOK_SKEW_SECONDS<0 or MAX_PENDING_LATENCY_TASKS<1 or MAX_TRIANGLES_PER_EXCHANGE<1 or MAX_UNIQUE_SYMBOLS_PER_EXCHANGE<3 or MAX_CONCURRENT_SUBSCRIPTIONS<1 or WS_SUBSCRIPTION_STAGGER_SECONDS<0 or RECONNECT_JITTER_SECONDS<0:raise ValueError("invalid freshness/concurrency/resource caps")

async def status_reporter(statuses):
 while True:
  await asyncio.sleep(STATUS_REPORT_INTERVAL_SECONDS);logger.info("STATUS: %s",", ".join(f"{k}={v['state']}(found={v['opportunities_found']},healthy={v.get('healthy_symbols',0)},stale={v.get('stale_symbols',0)},retrying={len(v.get('retrying',()))})" for k,v in statuses.items()))
async def main():
 validate_configuration();csv_log=CsvLogger();await csv_log.initialise();logger.info("PAPER ONLY: public WebSocket books, one metadata load; assumed fees=%s",TAKER_FEES)
 statuses={x:{"state":"starting","opportunities_found":0,"healthy_symbols":0,"stale_symbols":0,"retrying":set()} for x in EXCHANGE_IDS};tasks=[asyncio.create_task(run_exchange_scanner(x,statuses[x],csv_log)) for x in EXCHANGE_IDS];reporter=asyncio.create_task(status_reporter(statuses))
 try:await asyncio.gather(*tasks,return_exceptions=True)
 finally:
  for x in tasks+[reporter]:x.cancel()
  await asyncio.gather(*tasks,reporter,return_exceptions=True)
if __name__=="__main__":
 try:asyncio.run(main())
 except KeyboardInterrupt:logger.info("Scanner stopped cleanly.")
