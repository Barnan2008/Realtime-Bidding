# Orizon Auctions: real-time bidding

Live property auction. Node >= 22.5, **no npm install needed** (zero dependencies).

    npm start        # http://localhost:3000   (PORT=4000 npm start to change)
    npm test         # 40 simultaneous bidders race on one listing
    npm run reset    # wipe the database and re-seed listings

Open the page in two browser windows, pick different names and bid against each other.

## How it meets the requirements
- **Live push**: hand-written RFC 6455 WebSocket server (`/ws`). Every accepted bid, auction close and presence change is broadcast.
- **Race-safe bids**: each bid runs in one SQLite `BEGIN IMMEDIATE` transaction (write lock). It checks status, deadline, minimum next bid (current + increment) and the current leader before inserting. `UNIQUE(auction_id, amount)` is a second guard. Highest bid wins; stale or equal bids are rejected with the current price.
- **Persistence**: SQLite file `data.db` (WAL). Every connect or reconnect starts with a fresh `snapshot` read from the database, so reloads see the same state.
- **Disconnects / reconnects**: the client reconnects with backoff and resyncs from the snapshot. Bids carry a `requestId`, so a retry (including the REST fallback `POST /api/auctions/:id/bids`) never applies twice. The server pings every 20s and drops dead sockets. A per-auction `version` stops out-of-order updates overwriting newer state.
- **Extras**: auctions close automatically; a bid in the last 30s extends the deadline by 30s (anti-sniping).

## API
`POST /api/session {name}` -> `{user, token}` | `GET /api/auctions` | `POST /api/auctions/:id/bids {amount, requestId}` (Bearer token) | `GET /api/health`
WebSocket: connect to `/ws?token=...`, send `{type:"bid", reqId, auctionId, amount}`, receive `snapshot`, `auction`, `presence`, `ack`.
