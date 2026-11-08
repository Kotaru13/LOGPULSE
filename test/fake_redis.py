"""Минимальная in-memory имитация Redis для тестов (не трогает реальные данные)."""
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError


def _key(entry_id):
    a, _, b = str(entry_id).partition("-")  # "0" == "0-0", как в Redis
    return int(a), int(b or 0)


class FakePipeline:
    def __init__(self, backend, transaction=True):
        self._backend, self._ops = backend, []

    def __getattr__(self, name):
        method = getattr(self._backend, name)

        def queue(*args, **kwargs):
            self._ops.append((method, args, kwargs))
            return self
        return queue

    def execute(self):
        ops, self._ops = self._ops, []
        return [m(*a, **k) for m, a, k in ops]

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self._ops = []
        return False


class FakeRedis:
    def __init__(self):
        self.streams, self.groups = {}, {}
        self.hashes, self.zsets, self.strings, self.sets = {}, {}, {}, {}
        self._seq = 0

    # --- общие
    def ping(self):
        return True

    def pipeline(self, transaction=True):
        return FakePipeline(self, transaction)

    # --- Stream
    def xadd(self, name, fields, id="*", maxlen=None, approximate=True):
        self._seq += 1
        entry_id = f"{1_700_000_000_000 + self._seq}-0"
        self.streams.setdefault(name, []).append(
            (entry_id, {str(k): str(v) for k, v in fields.items()}))
        return entry_id

    def xlen(self, name):
        return len(self.streams.get(name, []))

    def xrevrange(self, name, max="+", min="-", count=None):
        items = [(i, dict(f)) for i, f in reversed(self.streams.get(name, []))]
        return items[:count] if count else items

    def xgroup_create(self, name, groupname, id="$", mkstream=False):
        if name not in self.streams:
            if not mkstream:
                raise ResponseError("ERR The XGROUP subcommand requires the key to exist")
            self.streams[name] = []
        if (name, groupname) in self.groups:
            raise ResponseError("BUSYGROUP Consumer Group name already exists")
        entries = self.streams[name]
        last = (0, 0) if id == "0" or not entries else _key(entries[-1][0])
        self.groups[(name, groupname)] = {"last": last, "pending": {}}
        return True

    def xreadgroup(self, groupname, consumername, streams, count=None, block=None,
                   noack=False):
        result = []
        for name, start in streams.items():
            group = self.groups.get((name, groupname))
            if group is None:
                raise ResponseError("NOGROUP No such consumer group")
            pending = group["pending"].setdefault(consumername, {})
            if start == ">":
                entries = [(i, f) for i, f in self.streams.get(name, [])
                           if _key(i) > group["last"]]
                entries = entries[:count] if count else entries
                for entry_id, fields in entries:
                    pending[entry_id] = fields
                    group["last"] = _key(entry_id)
                if entries:
                    result.append([name, [(i, dict(f)) for i, f in entries]])
            else:
                entries = sorted(((i, f) for i, f in pending.items()
                                  if _key(i) > _key(start)), key=lambda e: _key(e[0]))
                entries = entries[:count] if count else entries
                result.append([name, [(i, dict(f)) for i, f in entries]])
        return result

    def xack(self, name, groupname, *ids):
        group = self.groups.get((name, groupname))
        if group is None:
            return 0
        removed = 0
        for pend in group["pending"].values():
            for entry_id in ids:
                if pend.pop(entry_id, None) is not None:
                    removed += 1
        return removed

    def pending_count(self, name, groupname):  # вспомогательный метод для тестов
        group = self.groups.get((name, groupname), {"pending": {}})
        return sum(len(p) for p in group["pending"].values())

    # --- строки / множества
    def get(self, name):
        value = self.strings.get(name)
        return None if value is None else str(value)

    def incr(self, name):
        return self.incrby(name, 1)

    def incrby(self, name, amount=1):
        self.strings[name] = int(self.strings.get(name, 0)) + amount
        return self.strings[name]

    def sadd(self, name, *values):
        bucket = self.sets.setdefault(name, set())
        before = len(bucket)
        bucket.update(values)
        return len(bucket) - before

    def sismember(self, name, value):
        return value in self.sets.get(name, set())

    # --- Hash
    def hincrby(self, name, key, amount=1):
        h = self.hashes.setdefault(name, {})
        h[key] = int(h.get(key, 0)) + amount
        return h[key]

    def hgetall(self, name):
        return {k: str(v) for k, v in self.hashes.get(name, {}).items()}

    # --- Sorted Set
    def zadd(self, name, mapping):
        z = self.zsets.setdefault(name, {})
        added = sum(1 for m in mapping if m not in z)
        z.update({m: float(s) for m, s in mapping.items()})
        return added

    def zrevrange(self, name, start, end, withscores=False):
        items = sorted(self.zsets.get(name, {}).items(),
                       key=lambda kv: (kv[1], kv[0]), reverse=True)
        items = items[start:] if end == -1 else items[start:end + 1]
        return items if withscores else [m for m, _ in items]


class DownRedis:
    """Имитация недоступного Redis: любая команда -> ConnectionError."""

    def __getattr__(self, name):
        def fail(*args, **kwargs):
            raise RedisConnectionError("Redis недоступен (имитация в тесте)")
        return fail
