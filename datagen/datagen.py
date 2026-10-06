"""
datagen - mô phỏng hệ thống bán hàng: sinh vòng đời đơn hàng Olist liên tục
và phát mọi thay đổi thành sự kiện lên Redpanda.

    Faker + phân phối Olist thật  ──►  Redpanda (6 topic)  ──►  Spark Streaming

Thiết kế để chịu được việc dừng/khởi động lại bất kỳ lúc nào:

1. Đồng hồ mô phỏng chỉ chạy khi tiến trình chạy
   sim += min(khoảng_thật_từ_nhịp_trước, MAX_TICK) × TIME_SCALE
   Tiến trình dừng bao lâu thì đồng hồ cũng chỉ tiến tối đa MAX_TICK, nên
   khoảng thời gian nghiệp vụ (đặt hàng -> duyệt -> giao) luôn đúng phân
   phối, không bị kéo dài bởi thời gian hệ thống tắt.

2. Trạng thái bền trong SQLite trên volume: đồng hồ, hàng đợi bước kế tiếp
   của từng đơn, danh sách người mua. Khởi động lại là chạy tiếp, không mất
   đơn đang dở.

3. Transactional outbox: trong mỗi nhịp, sự kiện được ghi vào bảng outbox
   cùng transaction với việc tiến đồng hồ và cập nhật hàng đợi. Commit xong
   mới gửi Redpanda, nhận ack xong mới xoá khỏi outbox. Dừng ở bất kỳ điểm
   nào cũng không mất sự kiện; trường hợp xấu nhất là gửi lại, và event_id
   tất định cho phép phía tiêu thụ khử trùng.

Hai trục thời gian, không trộn lẫn:
    nghiệp vụ  (payload, sim_ts)      : đồng hồ mô phỏng -> phân tích, gold
    kỹ thuật   (event_ts, ingest_ts)  : đồng hồ thật     -> khử trùng, mốc cắt
"""

import hashlib
import json
import math
import os
import random
import signal
import sqlite3
import time
import uuid
from datetime import date, datetime, timedelta, timezone

import trino
import yaml
from confluent_kafka import Producer
from faker import Faker

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "redpanda:9092")
TIME_SCALE = float(os.environ.get("TIME_SCALE", "60"))
SIM_START = os.environ.get("SIM_START", "2018-10-18T00:00:00")
STATE_DB = os.environ.get("STATE_DB", "/state/datagen.sqlite")
SCENARIO = os.environ.get("SCENARIO", "/app/scenario.yml")
TRINO_HOST = os.environ.get("TRINO_HOST", "trino-coordinator")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))

TICK = 1.0       # giây thật giữa hai nhịp
MAX_TICK = 5.0   # trần giây thật tính cho một nhịp (chống nhảy cóc sau khi dừng)
FMT = "%Y-%m-%d %H:%M:%S"

TOPIC = {
    "customers": "olist.customers",
    "orders": "olist.orders",
    "order_items": "olist.order_items",
    "payments": "olist.payments",
    "reviews": "olist.reviews",
    "params": "olist.datagen_params",
}

STOP = False


def log(msg):
    print(f"[{datetime.now(timezone.utc).replace(tzinfo=None):%H:%M:%S}] {msg}", flush=True)


def ts(x):
    return None if x is None else x.strftime(FMT)


def eid(*parts):
    """event_id tất định: gửi lại cùng một thay đổi luôn ra cùng một id."""
    return hashlib.sha1(":".join(map(str, parts)).encode()).hexdigest()


def lognormal(rng, median, shape=0.5):
    return median * math.exp(rng.gauss(0, shape))


def pick(rng, mix):
    keys, weights = zip(*mix.items())
    return rng.choices(keys, weights=weights, k=1)[0]


# ===========================================================================
# Kịch bản: tham số động theo ngày mô phỏng
# ===========================================================================
class Scenario:
    def __init__(self, path, start_day):
        self.path, self.start_day = path, start_day
        self.mtime, self.cfg, self._noise = None, None, {}
        self.reload()

    def reload(self):
        m = os.path.getmtime(self.path)
        if m == self.mtime:
            return False
        with open(self.path, encoding="utf-8") as f:
            self.cfg = yaml.safe_load(f)
        self.mtime, self._noise = m, {}
        return True

    def campaigns(self, day):
        out = []
        for c in self.cfg.get("campaigns") or []:
            if date.fromisoformat(str(c["from"])) <= day <= date.fromisoformat(str(c["to"])):
                out.append(c)
        return out

    def _multiplier(self, key, day):
        m = 1.0
        for c in self.campaigns(day):
            m *= float((c.get("multiply") or {}).get(key, 1.0))
        return m

    def _ar1(self, name, n, sigma):
        """Nhiễu AR(1) tất định: x_k = 0.8 x_{k-1} + sigma*sqrt(1-0.64)*eps_k."""
        seq = self._noise.setdefault(name, [0.0])
        while len(seq) <= n:
            k = len(seq)
            eps = random.Random(f"{self.cfg['seed']}:{name}:{k}").gauss(0, 1)
            seq.append(0.8 * seq[-1] + sigma * 0.6 * eps)
        return seq[n]

    def value(self, name, day):
        spec = self.cfg["params"][name]
        v = float(spec["base"])
        n = max((day - self.start_day).days, 0)
        if "weekday" in spec:
            v *= spec["weekday"][day.weekday()]
        if "trend_per_30d" in spec:
            v *= (1 + spec["trend_per_30d"]) ** (n / 30)
        if "noise" in spec:
            v *= math.exp(self._ar1(name, n, spec["noise"]))
        v *= self._multiplier(name, day)
        return min(max(v, spec.get("min", v)), spec.get("max", v))

    def mix(self, name, day, base=None):
        raw = base if base is not None else self.cfg["mixes"][name]
        m = {k: float(w) * self._multiplier(f"{name}.{k}", day) for k, w in raw.items()}
        total = sum(m.values())
        return {k: w / total for k, w in m.items()}

    def snapshot(self, day):
        """Toàn bộ tham số của một ngày - gửi lên topic params để đối chiếu sau."""
        out = {k: round(self.value(k, day), 6) for k in self.cfg["params"]}
        out["payment_type"] = {k: round(v, 4) for k, v in self.mix("payment_type", day).items()}
        out["campaigns"] = [c["name"] for c in self.campaigns(day)]
        return out


# ===========================================================================
# Dữ liệu tham chiếu - lấy mẫu từ Olist thật trong lakehouse.bronze
# ===========================================================================
class Reference:
    def __init__(self):
        conn = trino.dbapi.connect(
            host=TRINO_HOST, port=TRINO_PORT, user="admin", source="datagen",
            catalog="lakehouse", schema="bronze",
        )
        cur = conn.cursor()

        def q(sql):
            cur.execute(sql)
            return cur.fetchall()

        # lấy nguyên dòng (mã bưu chính, thành phố, bang) để ba cột luôn nhất quán
        self.geo = q("SELECT customer_zip_code_prefix, customer_city, customer_state FROM customers")
        # người mua Olist -> địa chỉ (min_by theo customer_id để ba cột cùng
        # một dòng). Đơn của người mua quay lại dùng đúng địa chỉ này.
        self.buyers = {r[0]: (r[1], r[2], r[3]) for r in q("""
            SELECT customer_unique_id,
                   min_by(customer_zip_code_prefix, customer_id),
                   min_by(customer_city, customer_id),
                   min_by(customer_state, customer_id)
            FROM customers GROUP BY 1""")}
        self.unique_ids = list(self.buyers)
        # cặp sản phẩm - người bán, kèm giá và phí ship trung bình, trọng số = số lần bán
        pairs = q("""SELECT product_id, seller_id, avg(price), avg(freight_value), count(*)
                     FROM order_items GROUP BY 1, 2""")
        self.pairs = [p[:4] for p in pairs]
        self.pair_cw = list(_cumsum(p[4] for p in pairs))
        hours = dict(q("SELECT hour(order_purchase_timestamp), count(*) FROM orders GROUP BY 1"))
        mean = sum(hours.values()) / 24
        self.hour_factor = [hours.get(h, 0) / mean for h in range(24)]
        self.installments = dict(q("""SELECT payment_installments, count(*) FROM payments
                                      WHERE payment_type = 'credit_card' AND payment_installments > 0
                                      GROUP BY 1"""))
        rv = q("""SELECT o.order_delivered_customer_date > o.order_estimated_delivery_date,
                         r.review_score, count(*)
                  FROM reviews r JOIN orders o USING (order_id)
                  WHERE o.order_status = 'delivered' AND o.order_delivered_customer_date IS NOT NULL
                  GROUP BY 1, 2""")
        self.review_mix = {False: {}, True: {}}
        for late, score, n in rv:
            self.review_mix[bool(late)][int(score)] = n
        conn.close()

    def pair(self, rng):
        return rng.choices(self.pairs, cum_weights=self.pair_cw, k=1)[0]


def _cumsum(xs):
    t = 0
    for x in xs:
        t += x
        yield t


# ===========================================================================
# Trạng thái bền + outbox
# ===========================================================================
SCHEMA = """
CREATE TABLE IF NOT EXISTS clock   (k TEXT PRIMARY KEY, v TEXT);
CREATE TABLE IF NOT EXISTS pending (order_id TEXT PRIMARY KEY, step TEXT,
                                    due REAL, state TEXT);
CREATE INDEX IF NOT EXISTS pending_due ON pending(due);
CREATE TABLE IF NOT EXISTS buyers  (unique_id TEXT PRIMARY KEY, zip TEXT,
                                    city TEXT, state TEXT);
CREATE TABLE IF NOT EXISTS outbox  (id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    event_id TEXT UNIQUE, topic TEXT, key TEXT, value TEXT);
"""


class Generator:
    def __init__(self):
        os.makedirs(os.path.dirname(STATE_DB), exist_ok=True)
        self.db = sqlite3.connect(STATE_DB, isolation_level=None)
        self.db.executescript(SCHEMA)
        self.db.execute("PRAGMA journal_mode=WAL")
        row = self.db.execute("SELECT v FROM clock WHERE k='sim'").fetchone()
        self.sim = datetime.fromisoformat(row[0]) if row else datetime.fromisoformat(SIM_START)
        row = self.db.execute("SELECT v FROM clock WHERE k='last_day'").fetchone()
        self.last_day = date.fromisoformat(row[0]) if row else None
        self.start_day = datetime.fromisoformat(SIM_START).date()
        self.scenario = Scenario(SCENARIO, self.start_day)
        self.rng = random.Random()
        self.fake = Faker("pt_BR")
        self.producer = Producer({
            "bootstrap.servers": BOOTSTRAP,
            "linger.ms": 100,
            "compression.type": "lz4",
            "enable.idempotence": True,
            "acks": "all",
        })
        self.stats = {"orders": 0, "events": 0}

    # --- outbox ------------------------------------------------------------
    def emit(self, kind, key, event_type, seq, payload, event_id):
        value = json.dumps({
            "event_id": event_id,
            "event_type": event_type,
            "event_ts": datetime.now(timezone.utc).replace(tzinfo=None).strftime(FMT + ".%f")[:-3],
            "sim_ts": ts(self.sim),
            "seq": seq,
            "payload": {k: (None if v is None else str(v)) for k, v in payload.items()},
        })
        self.db.execute(
            "INSERT OR IGNORE INTO outbox(event_id, topic, key, value) VALUES (?,?,?,?)",
            (event_id, TOPIC[kind], key, value),
        )

    def flush_outbox(self):
        # Redpanda đang không phản hồi: còn tin cũ chưa gửi xong thì chưa đẩy
        # thêm, nếu không mỗi nhịp lại xếp lại toàn bộ outbox vào hàng đợi.
        if len(self.producer):
            self.producer.flush(5)
            if len(self.producer):
                return
        rows = self.db.execute("SELECT id, topic, key, value FROM outbox ORDER BY id").fetchall()
        if not rows:
            return
        sent = []

        def done(err, msg, rid):
            if err is None:
                sent.append(rid)

        for rid, topic, key, value in rows:
            self.producer.produce(topic, key=key, value=value,
                                  on_delivery=lambda e, m, rid=rid: done(e, m, rid))
        self.producer.flush(15)
        self.db.executemany("DELETE FROM outbox WHERE id=?", [(r,) for r in sent])
        self.stats["events"] += len(sent)

    # --- vòng đời đơn hàng ---------------------------------------------------
    def new_order(self, at, ref):
        """Tạo đơn mới tại thời điểm mô phỏng `at`: khách hàng, đơn, dòng hàng,
        rồi xếp bước "approve" vào hàng đợi."""
        sc, day, rng = self.scenario, at.date(), self.rng
        oid, cid = uuid.UUID(int=rng.getrandbits(128)).hex, uuid.UUID(int=rng.getrandbits(128)).hex
        if rng.random() < sc.value("repeat_buyer_rate", day):
            # Người mua quay lại (từ Olist hoặc do datagen tạo trước đó) giữ
            # nguyên địa chỉ cũ, để phân tích theo khách hàng và theo bang
            # nhất quán giữa các lần mua.
            own = self.db.execute("SELECT count(*) FROM buyers").fetchone()[0]
            k = rng.randrange(own + len(ref.unique_ids))
            if k < own:
                unique_id, zipc, city, state = self.db.execute(
                    "SELECT unique_id, zip, city, state FROM buyers LIMIT 1 OFFSET ?", (k,)
                ).fetchone()
            else:
                unique_id = ref.unique_ids[k - own]
                zipc, city, state = ref.buyers[unique_id]
        else:
            unique_id = uuid.UUID(int=rng.getrandbits(128)).hex
            zipc, city, state = rng.choice(ref.geo)
            # cùng transaction với outbox: người mua và sự kiện luôn nhất quán
            self.db.execute("INSERT INTO buyers VALUES (?,?,?,?)", (unique_id, str(zipc), city, state))
        self.emit("customers", cid, "customer_created", 1, {
            "customer_id": cid, "customer_unique_id": unique_id,
            "customer_zip_code_prefix": zipc, "customer_city": city, "customer_state": state,
        }, eid("customer", cid))

        approve_at = at + timedelta(minutes=lognormal(rng, sc.value("approve_delay_min", day)))
        est = at + timedelta(days=lognormal(rng, sc.value("estimated_delivery_d", day), 0.25))
        order = {
            "order_id": oid, "customer_id": cid, "order_status": "created",
            "order_purchase_timestamp": ts(at), "order_approved_at": None,
            "order_delivered_carrier_date": None, "order_delivered_customer_date": None,
            "order_estimated_delivery_date": est.strftime("%Y-%m-%d 00:00:00"),
        }
        self.emit("orders", oid, "order_created", 1, order, eid("order", oid, "created"))

        total = 0.0
        n_items = int(pick(rng, sc.mix("items_per_order", day)))
        pm = sc.value("price_multiplier", day)
        for i in range(1, n_items + 1):
            pid, sid, price, freight = ref.pair(rng)
            price = round(float(price) * pm * rng.uniform(0.9, 1.1), 2)
            freight = round(float(freight) * rng.uniform(0.9, 1.1), 2)
            total += price + freight
            self.emit("order_items", oid, "item_added", i, {
                "order_id": oid, "order_item_id": i, "product_id": pid, "seller_id": sid,
                "shipping_limit_date": ts(approve_at + timedelta(days=rng.uniform(4, 8))),
                "price": price, "freight_value": freight,
            }, eid("item", oid, i))

        state = {"order": order, "seq": 1, "total": round(total, 2)}
        self.db.execute("INSERT INTO pending VALUES (?,?,?,?)",
                        (oid, "approve", approve_at.timestamp(), json.dumps(state)))
        self.stats["orders"] += 1

    def advance(self, oid, step, due, state, ref):
        """Thực hiện một bước tới hạn của đơn và xếp lịch bước kế tiếp.

        approve -> ship -> deliver -> review, hoặc approve -> cancel /
        unavailable. Mỗi lần đổi trạng thái phát toàn bộ dòng đơn với `seq`
        tăng dần, nên phía nhận chỉ cần giữ bản có seq lớn nhất."""
        sc, rng = self.scenario, self.rng
        at = datetime.fromtimestamp(due)
        day = at.date()
        order = state["order"]

        def send(status, event_type):
            state["seq"] += 1
            order["order_status"] = status
            self.emit("orders", oid, event_type, state["seq"], order, eid("order", oid, event_type))

        def schedule(next_step, when):
            self.db.execute("UPDATE pending SET step=?, due=?, state=? WHERE order_id=?",
                            (next_step, when.timestamp(), json.dumps(state), oid))

        def finish():
            self.db.execute("DELETE FROM pending WHERE order_id=?", (oid,))

        if step == "approve":
            order["order_approved_at"] = ts(at)
            self._payments(oid, state["total"], day)
            send("approved", "order_approved")
            r = rng.random()
            cancel, unavail = sc.value("cancel_rate", day), sc.value("unavailable_rate", day)
            if r < cancel:
                schedule("cancel", at + timedelta(hours=lognormal(rng, 2)))
            elif r < cancel + unavail:
                schedule("unavailable", at + timedelta(hours=lognormal(rng, 24)))
            else:
                schedule("ship", at + timedelta(hours=lognormal(rng, sc.value("carrier_delay_h", day))))
        elif step == "ship":
            order["order_delivered_carrier_date"] = ts(at)
            send("shipped", "order_shipped")
            schedule("deliver", at + timedelta(hours=lognormal(rng, sc.value("delivery_delay_h", day))))
        elif step == "deliver":
            order["order_delivered_customer_date"] = ts(at)
            send("delivered", "order_delivered")
            if rng.random() < sc.value("review_rate", day):
                schedule("review", at + timedelta(hours=rng.uniform(0, 24)))
            else:
                finish()
        elif step in ("cancel", "unavailable"):
            send("canceled" if step == "cancel" else "unavailable", f"order_{step}")
            if rng.random() < 0.5:
                state["force_late"] = True
                schedule("review", at + timedelta(hours=rng.uniform(0, 48)))
            else:
                finish()
        elif step == "review":
            self._review(oid, order, state, at, ref)
            finish()

    def _payments(self, oid, total, day):
        sc, rng = self.scenario, self.rng
        n = int(pick(rng, sc.mix("payments_per_order", day)))
        shares = [rng.random() for _ in range(n)]
        s = sum(shares)
        for i in range(1, n + 1):
            ptype = pick(rng, sc.mix("payment_type", day)) if n == 1 or i == 1 else "voucher"
            inst = int(pick(rng, REF.installments)) if ptype == "credit_card" else 1
            self.emit("payments", oid, "payment_made", i, {
                "order_id": oid, "payment_sequential": i, "payment_type": ptype,
                "payment_installments": inst,
                "payment_value": round(total * shares[i - 1] / s, 2),
            }, eid("payment", oid, i))

    def _review(self, oid, order, state, at, ref):
        sc, rng, day = self.scenario, self.rng, at.date()
        late = state.get("force_late") or (
            order["order_delivered_customer_date"] is not None
            and order["order_delivered_customer_date"] > order["order_estimated_delivery_date"]
        )
        score = int(pick(rng, sc.mix("review_score", day, base=ref.review_mix[bool(late)])))
        rates = sc.cfg["comment_rate"]
        created = datetime(at.year, at.month, at.day)
        rid = uuid.UUID(int=rng.getrandbits(128)).hex
        self.emit("reviews", oid, "review_created", 1, {
            "review_id": rid, "order_id": oid, "review_score": score,
            "review_comment_title": self.fake.sentence(nb_words=3)[:40] if rng.random() < rates["title"] else None,
            "review_comment_message": self.fake.paragraph(nb_sentences=2) if rng.random() < rates["message"] else None,
            "review_creation_date": ts(created),
            "review_answer_timestamp": ts(created + timedelta(hours=rng.uniform(12, 72))),
        }, eid("review", rid))

    # --- một nhịp ------------------------------------------------------------
    def tick(self, real_dt, ref):
        """Một nhịp: tiến đồng hồ, sinh đơn mới, xử lý bước tới hạn - tất cả
        trong một transaction SQLite - rồi gửi outbox."""
        sc, rng = self.scenario, self.rng
        t0 = self.sim
        t1 = t0 + timedelta(seconds=min(real_dt, MAX_TICK) * TIME_SCALE)

        self.db.execute("BEGIN")
        # 1. sang ngày mô phỏng mới -> nạp lại kịch bản, công bố tham số của ngày
        day = t1.date()
        if self.last_day != day:
            if sc.reload():
                log(f"Đã nạp lại {SCENARIO}")
            self.sim = t1
            self.emit("params", day.isoformat(), "params", 1,
                      {"sim_date": day.isoformat(), "params_json": json.dumps(sc.snapshot(day))},
                      eid("params", day.isoformat()))
            self.last_day = day
            self.db.execute("INSERT OR REPLACE INTO clock VALUES ('last_day', ?)", (day.isoformat(),))

        # 2. đơn mới: số đơn ~ Poisson(tốc độ × khoảng thời gian mô phỏng)
        span = (t1 - t0).total_seconds()
        lam = sc.value("orders_per_day", t0.date()) / 86400 * ref.hour_factor[t0.hour] * span
        for at in sorted(t0 + timedelta(seconds=rng.uniform(0, span)) for _ in range(_poisson(rng, lam))):
            self.sim = at
            self.new_order(at, ref)

        # 3. các bước tới hạn của đơn cũ, theo đúng thứ tự thời gian
        for oid, step, due, state in self.db.execute(
            "SELECT order_id, step, due, state FROM pending WHERE due <= ? ORDER BY due",
            (t1.timestamp(),),
        ).fetchall():
            self.sim = datetime.fromtimestamp(due)
            self.advance(oid, step, due, json.loads(state), ref)

        self.sim = t1
        self.db.execute("INSERT OR REPLACE INTO clock VALUES ('sim', ?)", (t1.isoformat(),))
        self.db.execute("COMMIT")
        self.flush_outbox()


def _poisson(rng, lam):
    if lam > 30:
        return max(0, int(round(rng.gauss(lam, math.sqrt(lam)))))
    k, p, limit = 0, 1.0, math.exp(-lam)
    while True:
        p *= rng.random()
        if p <= limit:
            return k
        k += 1


REF = None


def main():
    global REF

    def stop(*_):
        global STOP
        STOP = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    gen = Generator()
    log(f"Đồng hồ mô phỏng: {gen.sim:%Y-%m-%d %H:%M:%S}  TIME_SCALE={TIME_SCALE:g}  "
        f"(1 ngày mô phỏng = {1440 / TIME_SCALE:.1f} phút thật)")
    log(f"Đơn đang dở trong hàng đợi: {gen.db.execute('SELECT count(*) FROM pending').fetchone()[0]}")

    while True:  # Trino có thể chưa sẵn sàng khi container vừa khởi động
        try:
            REF = Reference()
            break
        except Exception as exc:
            log(f"Chờ Trino ({exc.__class__.__name__}), thử lại sau 10 giây")
            time.sleep(10)
    log(f"Tham chiếu: {len(REF.geo):,} địa chỉ, {len(REF.unique_ids):,} người mua, "
        f"{len(REF.pairs):,} cặp sản phẩm-người bán")

    gen.flush_outbox()  # gửi nốt sự kiện còn kẹt từ lần chạy trước
    last, last_log = time.monotonic(), 0.0
    while not STOP:
        time.sleep(TICK)
        now = time.monotonic()
        gen.tick(now - last, REF)
        last = now
        if now - last_log >= 60:
            pending = gen.db.execute("SELECT count(*) FROM pending").fetchone()[0]
            log(f"sim {gen.sim:%Y-%m-%d %H:%M}  đơn mới {gen.stats['orders']:,}  "
                f"sự kiện đã gửi {gen.stats['events']:,}  đơn đang dở {pending:,}")
            last_log = now

    gen.flush_outbox()
    log(f"Dừng tại sim {gen.sim:%Y-%m-%d %H:%M:%S} - trạng thái đã lưu")


if __name__ == "__main__":
    main()
