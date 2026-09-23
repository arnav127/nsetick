# How the Matching Engine Works

NSE publishes every order message (each entry, modify and cancel) but not the order book
those messages build, and not the matching decisions the exchange made. `nsetick` rebuilds
the book by feeding the messages, in order, through a model of the exchange's matching
engine. This page explains that model from the ground up: what state it keeps, what each
message does to that state, when an order trades at once and when it waits, and the many
NSE-specific quirks that decide whether the rebuilt book is right.

The test of correctness is the exchange's own **trade file**. It lists every execution with
the order number of the buyer and of the seller. The engine produces the same kind of record,
so the two can be compared trade by trade. Every rule below was found that way: replay until
the first trade that differs from the exchange's, work out why, fix the rule, repeat. The
engine now reproduces the exchange's trade records essentially exactly; see
[Validation and Accuracy](Validation-and-Accuracy).

If you are new to order books, read the first two sections. If you already know price-time
priority, skip to [section 4](#4-entering-an-order-trade-now-rest-or-discard).

---

## 1. Background: what a limit order book is

A **limit order** says: *buy (or sell) up to this quantity, at this price or better.* Orders
that cannot trade straight away wait in the **book**, grouped by price:

```
        SELL side (asks)                 price    qty   orders in time order
        ---------------------------------------------------------------
                                         101.00   300   [#17 200] [#22 100]
        best ask  ->                     100.50   150   [#9 150]
        ---------------------------- spread ----------------------------
        best bid  ->                     100.00   400   [#4 250] [#11 100] [#30 50]
                                          99.50   500   [#2 500]
        BUY side (bids)
```

* The **best bid** is the highest price anyone will pay. The **best ask** is the lowest
  price anyone will sell at. The gap between them is the **spread**.
* An incoming order that reaches across the spread (a buy priced at or above the best ask,
  or a sell at or below the best bid) is **marketable**. It trades at once against the
  resting orders on the other side.
* Among resting orders, **better price goes first**. At the same price, **the order that
  arrived first goes first**. This is *price-time priority*. The waiting line at one price
  is the **queue**.
* A trade always happens at the **resting** order's price. A buy at 101.00 that meets an ask
  at 100.50 trades at 100.50. The incoming order gets the better price.

The order that arrives and trades is the **aggressor** (or *incoming* order). The order it
trades against is the **resting** (or *passive*) order.

## 2. What the engine keeps

For each security, the engine holds:

| State | What it is |
|---|---|
| **Price levels** | Two sorted maps (bids and asks) from price to a level. Each level stores its total visible and hidden quantity, and a first-in-first-out **queue** of order numbers. |
| **Resting orders** | For each order on the book: side, price, **visible** quantity, **hidden** quantity, **tranche** size and what is left of the current tranche (for icebergs), **remaining** total, and the participant flags (`algo_indicator`, `client_identity`). |
| **Pre-open orders** | Orders collected before the call auction, with their time priority (section 11). |
| **Closing price** | The 15:00–15:30 average of the engine's own trades, for the post-close session (section 11). |
| **Waiting stop-loss orders** | Stop orders that have not triggered yet. They are held *off* the book. |
| **Discarded remainders** | IOC and market orders whose unfilled part was thrown away, so the cancel the feed writes for it later can be recognised. |
| **Announced cancels** | Cancels read *ahead* of the current message, used for self-trade prevention (section 8). |

Prices are integer **paise** throughout, so comparisons are exact.

A cancelled order is not searched for in its queue. It is deleted from the order map, and its
queue entry is left behind as a **tombstone** that is skipped when it reaches the front.
Each placement also carries a sequence number. When a modify re-enters the same order number,
its old queue entry is recognised as stale and skipped too.

## 3. The three messages

The orders file has one record per message, with an **activity type**:

| Type | Meaning | What the engine does |
|---|---|---|
| `1` | **Entry**: a new order (or a stop-loss order's trigger; see section 7) | Match it; rest or discard what is left |
| `3` | **Cancel** | Remove the order from wherever it is |
| `4` | **Modify** | Change the order: in place, or by removing and re-entering it |

Which rules apply depends on the time of day:

| Time | Session | What happens |
|---|---|---|
| 09:00 – about 09:08 | **Pre-open call auction** | Orders are collected, not matched; then all matched at one price (section 11) |
| 09:08 – 09:15 | Buffer | Unfilled market orders from the auction become limit orders; nothing trades |
| 09:15 – 15:30 | **Continuous trading** | Everything in sections 4 to 10 |
| from 15:40 | **Post-close session** | Every order trades at the closing price, in time order (section 11) |

In the continuous session, the top-level flow for one message:

```mermaid
flowchart TD
    M([Message from the feed]) --> T{Activity type}

    T -->|1 Entry| E1{Order number is a<br/>waiting stop-loss?}
    E1 -->|yes: this is its trigger| ENTER[[Enter as a live order]]
    E1 -->|no| E2{Stop-loss flag set<br/>and a positive trigger price?}
    E2 -->|yes| HOLD[Hold off the book<br/>as a waiting stop]
    E2 -->|no| ENTER

    T -->|3 Cancel| C1{Withdrawn earlier by<br/>self-trade prevention?}
    C1 -->|yes| DONE1[Nothing to do:<br/>this is its cancel]
    C1 -->|no| C2{Waiting stop-loss?}
    C2 -->|yes| DROP[Drop it: it was<br/>never on the book]
    C2 -->|no| C3{Resting on<br/>the book?}
    C3 -->|yes| REMOVE[Remove it and its<br/>hidden quantity]
    C3 -->|no| C4{IOC or market order<br/>whose remainder<br/>was discarded?}
    C4 -->|yes| DONE2[Expected: count as<br/>remainder cancel]
    C4 -->|no| UNK[Count as unknown<br/>order reference]

    T -->|4 Modify| MOD[[Modify rules, section 6]]
    T -->|anything else| REJ[Reject]
```

---

## 4. Entering an order: trade now, rest, or discard

This is the heart of the engine. When an order enters (a new order, a triggered stop, or a
modify that re-enters), four things happen in sequence.

```mermaid
flowchart TD
    A([Order enters]) --> V{Quantity positive, and price<br/>positive or a market order?}
    V -->|no| R[Reject]
    V -->|yes| MK{Market order?}
    MK -->|yes| INF[Match as if its limit were<br/>infinitely aggressive:<br/>buy at +inf, sell at 0]
    MK -->|no| LIM[Match at its own limit]
    INF --> LOOP[[Matching loop, section 5]]
    LIM --> LOOP
    LOOP --> Q{Anything left?}
    Q -->|no| F([Fully filled: done])
    Q -->|yes| IOC{IOC or market order?}
    IOC -->|yes| DISC([Discard the remainder.<br/>The feed will cancel it<br/>15-31 us later])
    IOC -->|no| REST([Rest at its limit price,<br/>at the BACK of the queue.<br/>Icebergs show one tranche])
```

**Step 1: validate.** A non-positive quantity is rejected. So is a zero or negative price,
*unless* it is a market order (see below).

**Step 2: match.** While the order is still marketable, it trades against the other side, best
price first (section 5). This is the **immediate execution**.

**Step 3: what is left.** If the order is filled completely, it never touches the book. If
some quantity is left, the order type decides what happens to it:

| Order type | Unfilled quantity |
|---|---|
| Ordinary limit order (day order) | **Rests** at its limit price, at the **back** of that price's queue. |
| **IOC** (immediate-or-cancel) | **Discarded.** It never rests, not even for a microsecond. |
| **Market** order | **Discarded**, like IOC. A market order that finds no liquidity simply does nothing. |
| **Iceberg** (disclosed quantity) | Rests. Only one tranche is **visible**; the rest is **hidden** behind it (section 9). |

**Step 4: remember discards.** NSE follows each discarded IOC or market remainder with an
explicit cancel message, 15–31 µs later. The engine notes the order number, so that cancel is
recognised as expected rather than flagged as unknown.

### Market orders

In the orders file, a market order has `mkt_order_flag = Y` and a **limit price of 0**. Treating
that zero as a real price would make a market *buy* unable to buy anything. Earlier versions
rejected these orders as invalid, which dropped 3.4% of all entries: the most aggressive flow
in the session. The engine instead matches a market buy as if its limit were infinite, and a
market sell as if its limit were zero. It walks the book until it is filled or the other side
is empty. Each fill still prints at the resting order's price.

### IOC orders

IOC means *trade what you can now, cancel the rest*. The feed shows this clearly: every IOC
that is unfilled or partly filled is followed by a cancel within about 15–31 µs, and fully
filled IOCs never are. If the engine let the remainder rest until that cancel arrived, orders
arriving in those microseconds could trade against liquidity that was never really there. So
the remainder is discarded at once.

### When does an order go to the back of the queue?

| Situation | Queue position |
|---|---|
| New order rests | Back of the queue at its price |
| Partially filled, still showing quantity | **Keeps** its position at the front |
| Iceberg reveals its next tranche | **Back** of the queue (section 9) |
| Modify at the same price that does **not** increase what the order shows | **Keeps** its position |
| Modify at the same price that increases what the order shows | **Back** |
| Modify: price changed | **Back**, at the new price |
| Stop-loss triggers | Back of the queue, timed from the trigger |

---

## 5. The matching loop

The incoming order works through the opposite side one price level at a time, and through each
level one order at a time:

```mermaid
flowchart TD
    S([Incoming order, quantity left = Q]) --> BL{Opposite side has<br/>a best price P?}
    BL -->|no| OUT([Stop: nothing to trade with])
    BL -->|yes| CR{Still crossed?<br/>buy: limit at or above P<br/>sell: limit at or below P}
    CR -->|no| OUT2([Stop: no longer marketable])
    CR -->|yes| FR{Front of the queue at P}
    FR -->|queue empty| DL[Delete level P] --> BL
    FR -->|tombstone or stale entry| POP[Pop it] --> FR
    FR -->|live order O| STP{A cancel in this<br/>step's slot?<br/>section 8}
    STP -->|cancel of O| WD[Withdraw O without trading.<br/>Step + 1; the next trade<br/>is a new record] --> FR
    STP -->|cancel of the incoming order| STOPIN([Stop: the incoming order<br/>is cancelled, nothing rests])
    STP -->|no| VIS{O shows any<br/>visible quantity?}
    VIS -->|no, iceberg with hidden left| REV[Reveal next tranche,<br/>move O to BACK] --> FR
    VIS -->|yes| TR["Trade min(Q, visible) at P,<br/>the resting price"]
    TR --> MG{Same pair and price as the<br/>previous fill, nothing<br/>printed in between?}
    MG -->|yes| ADD[Add to that trade.<br/>O then shows a fresh<br/>full tranche: section 9]
    MG -->|no| NEW[New trade record.<br/>Sweep step + 1]
    ADD --> AFT{O's state now}
    NEW --> AFT
    AFT -->|fully filled| RM[Remove O] --> DONE{Quantity left?}
    AFT -->|tranche used up,<br/>hidden quantity left| REV2[Reveal next tranche,<br/>move O to BACK] --> DONE
    AFT -->|still showing quantity| DONE
    DONE -->|yes| FR
    DONE -->|no| FIN([Incoming order filled])
```

Points worth noticing:

* **Levels are swept in price order.** A large buy takes everything at the best ask, then moves
  to the next ask, and so on. It keeps going until it is filled or the next ask is above its
  limit.
* **Every fill is at the resting price.** A buy limited at 102 that sweeps asks at 100, 101 and
  102 produces trades at 100, 101 and 102.
* **A partly filled resting order keeps its place.** If the incoming order takes 30 of a resting
  order's 100, the other 70 stays at the front.
* **Only visible quantity trades directly.** Hidden iceberg quantity can trade, but only after it
  has been revealed as a tranche, and that tranche joins the back of the queue.

### One trade per pair

The exchange writes **one trade record per (incoming, resting) pair per price** in a sweep.
Say an incoming order eats through an iceberg that is alone at its price. Each revealed
tranche goes to the back of a queue that has nothing else in it, so it is immediately at the
front again. The exchange prints the whole execution as **one** trade. The engine merges
consecutive fills of the same pair at the same price in the same way.

The record ends when anything else is printed in between: a trade with another order, or a
self-trade-prevention cancel (section 8), which is a message of its own. After that, a further
fill between the same two orders is a new record. That matters beyond counting records: it
decides how much of the iceberg shows afterwards (section 9).

---

## 6. Modify

A modify record (activity type 4) carries the order's **new state**. It includes the new
price and the **remaining** quantity, not the original total: fills since entry have already
been subtracted.

```mermaid
flowchart TD
    M([Modify for order N]) --> SLN{Modify sets the stop flag<br/>with a trigger price?}
    SLN -->|yes| RE[Take N off the book if it is live<br/>and hold it as a WAITING stop]
    SLN -->|no| W{N is a waiting<br/>stop-loss?}
    W -->|yes| CONV[Convert: enter as an<br/>ordinary live order]
    W -->|no| AM{On the book at the same price,<br/>and what it shows does<br/>not increase?}
    AM -->|yes| INP([Amend in place:<br/>KEEP queue position])
    AM -->|no| LIVE{On the book?}
    LIVE -->|yes| CTE([Cancel-then-enter:<br/>remove, re-enter as a new order,<br/>BACK of the queue.<br/>It can trade immediately])
    LIVE -->|no| UNK([Not known: enter it anyway<br/>and count an unknown reference])
```

**Queue position.** At the same price, an order keeps its place **unless the quantity it
displays grows**. Other traders only see the displayed quantity, so a change that doesn't
show more doesn't jump anyone. So:

* reducing an ordinary order: keeps its place;
* raising an ordinary order: back of the queue;
* an iceberg cutting its disclosed quantity (showing less): keeps its place;
* an iceberg raising its disclosed quantity so that it shows more: back of the queue;
* an iceberg raising its *total* but keeping its disclosed quantity: keeps its place, since
  what it shows is unchanged;
* any price change: back of the queue, at the new price.

**What an iceberg shows after a modify.** The exchange keeps counting down the current
tranche through a modify, as long as the disclosed quantity stays the same. Change the
disclosed quantity and it starts a fresh tranche, with two refinements, both measured:

| Modify | Shows afterwards |
|---|---|
| Same disclosed quantity | What was left of the current tranche |
| Disclosed quantity cut to exactly the quantity remaining before the modify | What was left of the current tranche |
| Either of those, but raising the quantity of an order that was showing all it had left (for an unchanged disclosed quantity: an order down to less than one tranche) | A fresh full tranche |
| Any other change of disclosed quantity | A fresh full tranche of the new size |

These tables are what made the difference between about 99% and 100% agreement on orders
that modify often (algorithmic icebergs re-price many times a second).

**A re-entered order goes through the full entry path.** If its new price crosses the spread,
it trades immediately, exactly like a new order, and those trades count against its tranche
(section 9).

**A modify of an unknown order** happens legitimately in one case: the exchange converts an
unfilled market order to a limit order with a modify (after the pre-open auction, for
instance). The engine enters it and does not count the conversion as an error.

---

## 7. Stop-loss orders

A stop-loss order is dormant until the market reaches its **trigger price**. It then becomes
an ordinary limit (or market) order. The feed marks such orders `stop_loss_flag = Y` with a
`trigger_price`.

Crucially, the **exchange** decides when a stop triggers, and it **writes a second entry
record** for the same order number at that moment. No ordinary order ever has two entry
records. The engine does **not** try to work out triggers itself from the prices it has
matched. That was tried: small differences in the replayed last price fire stops a few
trades early, and the stop then takes liquidity that was really taken by someone else. It
simply follows the feed.

```mermaid
stateDiagram-v2
    [*] --> Waiting: Entry with stop flag and trigger > 0
    [*] --> Live: Entry without stop flag
    Waiting --> Live: Second Entry record for the same order (triggered)
    Waiting --> Live: Modify clearing the stop flag (converted)
    Waiting --> Waiting: Modify keeping the stop flag (new trigger or limit)
    Live --> Waiting: Modify setting the stop flag (re-armed, leaves the book)
    Live --> Live: Modify (amend in place or cancel-then-enter)
    Live --> Filled: Traded to zero
    Waiting --> Cancelled: Cancel
    Live --> Cancelled: Cancel
    Filled --> [*]
    Cancelled --> [*]
```

A waiting stop is **not** on the book. It adds no depth, is invisible in snapshots, and cannot
be traded against. When it triggers, it enters with time priority from the trigger, and can
trade immediately if its limit is marketable.

---

## 8. Self-trade prevention (STP)

Exchanges do not let a client trade with itself. At NSE, when an incoming order would match a
resting order belonging to the **same client**, the exchange cancels one of the two instead.
Usually it is the **resting** order, and the incoming order carries on to the next order in
the queue. Sometimes it is the **incoming** order, which then stops: it trades no further and
nothing of it rests.

The difficulty is that the orders file **has no client identifier**. All the replay can see
is the result: a cancel of the resting order, written by the exchange *during* the incoming
order's sweep. Without handling this, the replay would trade the two orders, since the cancel
appears in the file *after* the entry that caused it. That puts phantom trades and wrong queue
positions into the book.

### The feed's clock gives it away

The exchange stamps messages in **jiffies** of 1/65536 s (≈ 15.26 µs). When one incoming
order produces several messages (trades, and cancels of self-trade-prevented orders), each is
stamped **one jiffy after the previous one**:

```
 t0             incoming order entry                     (timestamp in the orders file)
 t0 + 1 jiffy   message for sweep step 0   e.g. trade with resting order A
 t0 + 2 jiffies message for sweep step 1   e.g. STP cancel of resting order B   <- orders file
 t0 + 3 jiffies message for sweep step 2   e.g. trade with resting order C
 ...
 t0 + (k+1) jiffies   message for step k
```

So an STP cancel is not simply "a cancel soon after". It is a cancel stamped **exactly at the
step of the sweep where the incoming order reached that resting order**.

```mermaid
sequenceDiagram
    participant In as Incoming buy 900, at t0
    participant A as Resting sell 101
    participant B as Resting sell 102, same client as 900
    participant C as Resting sell 103
    In->>A: step 0, stamped t0 + 1 jiffy - trade
    In-->>B: step 1, stamped t0 + 2 jiffies - exchange cancels 102
    In->>C: step 2, stamped t0 + 3 jiffies - trade
    Note over B: The orders file shows a cancel<br/>of 102 at t0 + 2 jiffies
```

### What the engine does

1. **Read ahead.** The replay reads the feed up to **100 ms** ahead of the message it is
   applying. Every cancel it sees is *announced* to the book (`OrderBook::announce`). 100 ms
   covers sweeps of several thousand steps.
2. **Count the sweep.** While matching an incoming order, the engine counts the steps: each
   new trade record and each STP withdrawal is one step.
3. **Check each resting order before trading it.** Look for a cancel in this step's slot,
   `(step + 1) × jiffy` after the incoming order, to within half a jiffy (timestamps are
   truncated to whole microseconds):
   * a cancel of the **resting order** there: withdraw it without trading, and carry on;
   * a cancel of the **incoming order** there: stop. Nothing more trades and nothing rests.

   The file has no client identifier, and neither the algo flag nor the participant category
   (`client_identity`) will do as one: the exchange prevents trades between a client's
   algorithmic and manual orders, and between its custodian and non-custodian orders, and
   filtering on either hid genuine preventions. The cancel's slot is the evidence. Coming back
   to the order the incoming order is already trading with (its next tranche) is a
   continuation, never STP.
4. When the withdrawn order's own cancel is later applied, the engine recognises it as already
   handled.

Why so exact? Traders often cancel their own orders soon after a partial fill, a few hundred
microseconds later. A loose "cancelled within 1 ms" rule withdrew those as well, before they
had in fact traded. It also missed genuine STP cancels deep inside large sweeps, stamped 2–4
ms after the entry. And an IOC's ordinary remainder cancel lands exactly one slot after its
last trade, which a tolerance of more than half a jiffy mistakes for STP. Slot timing, to half
a jiffy, tells them all apart.

Callers that never announce events get the plain textbook behaviour: every resting order can
be matched. The replay in `nsetick book` and `replay_fills` always announces.

---

## 9. Icebergs (disclosed quantity)

An order with `0 < volume_disclosed < volume_original` is an **iceberg**. It shows only
`volume_disclosed` (one **tranche**) at a time. The rest is real liquidity that nobody else
can see.

**How much shows.** The exchange counts down what is left of the current tranche. Every
**trade record** the order takes part in counts against it, whether the order is resting or
is the one arriving:

* a trade that takes **the whole tranche or more** leaves a **fresh full tranche** showing
  (or the whole remainder, if that is less);
* a **smaller** trade just reduces what shows.

```mermaid
stateDiagram-v2
    [*] --> Showing: Enters with one full tranche (its own trades on entry count against it)
    Showing --> Showing: A trade smaller than what shows (keeps queue position)
    Showing --> Refilled: A trade uses up what shows
    Refilled --> Showing: A fresh full tranche shows and the order moves to the BACK of the queue
    Showing --> Showing: A trade runs on into the next tranche (alone at its price): a fresh full tranche shows
    Showing --> Done: Remaining reaches 0, or cancelled
    Done --> [*]
```

Three consequences that are easy to get wrong, each of which cost measurable accuracy until it
was found:

* **A trade that runs on into the next tranche leaves a full tranche, not the leftover.** An
  iceberg showing 10 that is hit for 15 in one trade (possible when it is alone at its price)
  then shows 10, not 5.
* **Its own trades count.** An iceberg that enters showing 10 and trades 3 on arrival rests
  showing 7, not 10.
* **Only records count, not fills.** If a self-trade-prevention cancel splits one incoming
  order's trades with the iceberg into two records, the second record is an ordinary small
  trade (section 5).

**Replenishment loses priority.** Every fresh tranche joins the **back** of the queue at its
price, behind orders that arrived after the iceberg did. (Keeping it at the front instead
drops exact trade matches from about 95% to about 65%.)

**Snapshots report both parts.** `bid_qty_k` / `ask_qty_k` are what other participants saw.
`bid_hidden_k` / `ask_hidden_k` are what was really there.

**Modifies** can keep the current tranche's count or start a fresh one; see section 6.

---

## 10. A worked example

The book starts with three sell orders at 100.00 and one at 100.50 (quantities in shares):

| Queue at 100.00 | Order | Visible | Hidden | Notes |
|---|---|---|---|---|
| 1st | #1 | 100 | 400 | iceberg, tranche 100 |
| 2nd | #2 | 50 | 0 | the buyer's own order; the file has its cancel at t0+2j |
| 3rd | #3 | 200 | 0 | |
| at 100.50 | #4 | 300 | 0 | |

A **buy limit 100.50 for 500** (#9) arrives at t0:

| Step | Stamp | What happens | Buy left |
|---|---|---|---|
| 0 | t0+1j | Trade #9 × #1: 100 @ 100.00. #1's tranche is used up: reveal 100, #1 moves behind #3 | 400 |
| 1 | t0+2j | Front is now #2. A cancel for #2 is announced at t0+2j, in this step's slot, and the client could match. **Withdrawn**, no trade | 400 |
| 2 | t0+3j | Trade #9 × #3: 200 @ 100.00 | 200 |
| 3 | t0+4j | Trade #9 × #1 (its second tranche): 100 @ 100.00. Reveal again, #1 to the back (it is now alone) | 100 |
| (merged) | | #1 is at the front again. Trade #9 × #1: 100 more @ 100.00, **merged into step 3's trade** (same pair and price, nothing in between) | 0 |

The trade file shows three trades: #9×#1 100, #9×#3 200, #9×#1 200, all at 100.00, and no
trade with #2. Afterwards #1 has 200 left and shows a fresh tranche of 100: the last trade ran
on into a new tranche. The ask at 100.50 is untouched, because the order was filled before it got
there. (j is one jiffy; #2's cancel counts as STP because it sits in the slot of step 1,
the step at which the sweep reached #2.)

Had the buy been an **IOC for 1,200**, it would have taken all 700 at 100.00, #1's
remaining tranches included, then 300 at 100.50 (still within its limit). The last 200 would
have been discarded, not rested at 100.50.

---

## 11. The pre-open auction and the post-close session

### Pre-open call auction

From 09:00 NSE collects orders without matching them. Order entry closes at a random moment
between 09:07 and 09:08, and all orders are then matched at **one price**. The engine
reproduces the exchange's auction trades exactly, trade for trade, in all 240 security-sessions
examined (90,736 trades):

1. **The equilibrium price** is the limit price at which the most quantity can trade. If
   several prices tie, the one leaving the smallest imbalance between buying and selling. If
   they still tie, the one closest to the **previous day's close** (checked on every tied
   session whose previous day is in the data).
2. **Who trades:** buys at or above that price, sells at or below it, and every market order.
3. **In what order:** buys best price first, sells best price first; **market orders after
   every limit order** (in effect at the equilibrium price, behind the limit orders there);
   within a price, earlier first. The two lists are paired off in order, each trade the
   smaller of the two remainders.
4. **Time priority through a pre-open modify** follows the same idea as in section 6: an order
   keeps its time if the modify leaves its price unchanged and does not raise its quantity.
5. **What is left** joins the continuous book at its limit in that priority order. An
   unfilled market order does not: moments later the feed shows the exchange converting it to
   a limit order at the equilibrium price, and that modify enters it.

The engine knows the auction has happened from the time (09:08) or from that first conversion
modify. A trader can also turn a market order into a limit order before the close, so a
conversion is recognised only if the auction, run as things stand, prices exactly at the
modify's limit and leaves exactly the modify's quantity unfilled.

The previous close is not in a day's files, but the feed usually settles a tie anyway: the
conversions of unfilled market orders carry the exchange's equilibrium price, and the engine
takes the price from the first one. That settled every tie in the 240 security-sessions
checked. When there is nothing to convert, pass the previous close with `previous_close=`
(Python) or `--previous-close` (CLI); without it the lowest candidate price is taken, which
changes only the auction's price.

### Post-close session

After continuous trading ends at 15:30, orders are cancelled and, from 15:40, orders are taken
at the **closing price** only. Every order, market orders included, joins one queue at that
price and trades in time order. The closing price is the volume-weighted average of trades from
15:00 to 15:30, **truncated to whole paise, then rounded to the nearest tick** (5 paise): exact
in all 240 security-sessions checked. The engine computes it from its own trades.

## 12. What the engine does not model

* **Price bands, circuit breakers, trading halts, freeze-quantity rejections.** Orders the
  exchange rejected never appear in the orders file, so no model of these is needed.
* **Client identity.** STP is inferred from the cancel's timing alone. A trader's own cancel
  landing by coincidence in exactly the right half-jiffy slot would be taken for STP. No such
  case turned up in the data checked.
* **The previous close** (above).

## 13. Counters

`OrderBook::stats()` (and the snapshot `interval_*` columns) expose what the engine did. They
are useful for checking a replay:

| Counter | Meaning |
|---|---|
| `entries`, `cancels`, `modifies` | Messages of each type applied |
| `events_rejected` | Unusable messages: non-positive quantity, zero price on a non-market order, unknown type |
| `unknown_order_refs` | Cancels or modifies naming an order the book never saw. A few at the start are normal; many means something is wrong |
| `trades_generated`, `volume_matched` | Trade records produced, and shares matched |
| `replenishments` | Iceberg tranches revealed |
| `market_orders` | Market orders entered |
| `unrested_quantity` | IOC and market quantity discarded unfilled |
| `remainder_cancels` | The feed's cancels for those discarded remainders (expected) |
| `amended_in_place` | Modifies that kept queue priority |
| `stops_held` | Stop-loss orders put into the waiting state |
| `stops_triggered` | Stops that entered the book, by trigger record or conversion |
| `self_trade_preventions` | Orders withdrawn by STP, resting or incoming |

## 14. Check it yourself

```python
import nsetick
fills = nsetick.replay_fills("parsed/segment=cm/kind=orders/date=2022-01-25", symbols=["TCS"])
# pyarrow Table: symbol, txn_time, trade_price, trade_quantity,
#                buy_order_number, sell_order_number, aggressor
```

Join it with the parsed trade file on `(buy_order_number, sell_order_number)`, or run the
ready-made comparison:

```bash
python tools/verify_replay.py parsed/            # every session and security found
python tools/verify_replay.py parsed/ --symbols TCS INFY --dates 2022-01-25
```

See [Validation and Accuracy](Validation-and-Accuracy) for what the numbers mean and the
results across many sessions.
