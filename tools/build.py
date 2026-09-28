#!/usr/bin/env python3
"""Build the Temporal Engineering Manual (single self-contained HTML file).

Inputs
  data/annot.json   descriptor-driven annotations of real captured bytes
                    (produced by tools/annot over blobs captured by tools/probe
                    and read out of the SQLite persistence file).
  tools/content/*.html  manual prose (concatenated in name order) with placeholders:
                    <!--DUMP key|caption|opts-->   annotated hex dump figure
                    <figure class="fig" ...>        numbered automatically
Output
  temporal-technical-manual.html
"""
import html, json, os, re, sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
A = json.load(open(os.path.join(ROOT, "data", "annot.json")))

WIRE = {0: "VARINT", 1: "I64", 2: "LEN", 5: "I32"}
SCALAR_STR = {"string"}
LEAF_BYTES = {"bytes"}


def esc(s):
    return html.escape(str(s), quote=True)


def classify(tree):
    """Return per-byte role list, per-byte leaf index, and flat leaf table."""
    leaves = []
    roles = {}
    owner = {}

    def rec(nodes, path, depth):
        for n in nodes:
            name = n["f"]
            p = f"{path}.{name}" if path else name
            o, tl, ll, e = n["o"], n["tl"], n["ll"], n["e"]
            kids = n.get("k")
            idx = len(leaves)
            val = n.get("v", "")
            t = n.get("t", "")
            if kids is not None or (n["w"] == 2 and t and "." in t and not kids):
                # message-typed field: header row, then children
                leaves.append(dict(path=p, n=n["n"], w=n["w"], o=o, e=e, v=val, t=t, depth=depth, kind="msg", hdr=True))
                for i in range(o, o + tl):
                    roles[i] = "tag"; owner[i] = idx
                for i in range(o + tl, o + tl + ll):
                    roles[i] = "len"; owner[i] = idx
                if kids:
                    rec(kids, p, depth + 1)
                continue
            if n["w"] == 2:
                kind = "str" if t == "string" else ("bytes" if t == "bytes" else "raw")
                if kind == "bytes" and val.startswith('"'):
                    kind = "str"
                if val.startswith("packed"):
                    kind = "int"
            elif t.startswith("temporal.") or t.startswith("google."):
                kind = "enum"
            elif t == "bool":
                kind = "enum"
            else:
                kind = "int"
            if n.get("kind"):
                kind = n["kind"]
            if name == "seconds" and val.lstrip("-").isdigit() and (1e9 < int(val) < 4e9 or int(val) < -6e10):
                import datetime
                val = val + " (" + (datetime.datetime(1970, 1, 1) + datetime.timedelta(seconds=int(val))).isoformat() + "Z)"
            leaves.append(dict(path=p, n=n["n"], w=n["w"], o=o, e=e, v=val, t=t, depth=depth, kind=kind, hdr=False))
            for i in range(o, o + tl):
                roles[i] = "tag"; owner[i] = idx
            for i in range(o + tl, o + tl + ll):
                roles[i] = "len"; owner[i] = idx
            for i in range(o + tl + ll, e):
                roles[i] = kind; owner[i] = idx

    rec(tree, "", 0)
    return roles, owner, leaves


def short_type(t):
    if not t:
        return ""
    return t.split(".")[-1]


def synth(key, msg, b, fields):
    """Register a hand-specified (non-protobuf) byte layout for dumping."""
    tree = []
    for i, (s_, e_, name, kind, val) in enumerate(fields):
        tree.append({"o": s_, "tl": 0, "ll": 0, "e": e_, "n": i + 1, "w": -1, "f": name, "t": "", "v": val, "kind": kind})
    A[key] = {"msg": msg, "hex": b.hex(), "tree": tree}


def add_synthetic():
    hdr = bytes.fromhex(
        "53514c69746520666f726d6174203300100002020040202000000002000000a3"
        "00000000000000000000006d0000000400000000000000000000000100000000"
        "0000000000000000000000000000000000000000000000000000000000000002"
        "002e95c9")
    synth("sqlite_header", "SQLite database header (first 100 B of run/temporal.db)", hdr, [
        (0, 16, "magic", "str", '"SQLite format 3\\0"'),
        (16, 18, "page_size (BE u16)", "int", "0x1000 = 4096"),
        (18, 19, "file_format_write_version", "enum", "2 = WAL"),
        (19, 20, "file_format_read_version", "enum", "2 = WAL"),
        (20, 21, "reserved_bytes_per_page", "int", "0"),
        (21, 22, "max_embedded_payload_fraction", "int", "64 (must be 64)"),
        (22, 23, "min_embedded_payload_fraction", "int", "32 (must be 32)"),
        (23, 24, "leaf_payload_fraction", "int", "32 (must be 32)"),
        (24, 28, "file_change_counter", "int", "2"),
        (28, 32, "database_size_in_pages", "int", "0xa3 = 163 pages (652 KiB)"),
        (32, 36, "first_freelist_trunk_page", "int", "0"),
        (36, 40, "freelist_page_count", "int", "0"),
        (40, 44, "schema_cookie", "int", "0x6d = 109"),
        (44, 48, "schema_format_number", "int", "4"),
        (48, 52, "default_page_cache_size", "int", "0"),
        (52, 56, "largest_root_btree_page", "int", "0 (no auto-vacuum)"),
        (56, 60, "text_encoding", "enum", "1 = UTF-8"),
        (60, 64, "user_version", "int", "0"),
        (64, 68, "incremental_vacuum", "int", "0"),
        (68, 72, "application_id", "int", "0"),
        (72, 92, "reserved_for_expansion", "raw", "20 × 00"),
        (92, 96, "version_valid_for", "int", "2"),
        (96, 100, "sqlite_version_number", "int", "0x002e95c9 = 3053001 → 3.53.1"),
    ])
    ns = "73ab11db-a831-4574-b804-b43fedd412e7"
    k = (ns + "_order-1001").encode()
    synth("shard_key", "WorkflowIDToHistoryShard input bytes", k, [
        (0, 36, "namespaceID (string form)", "str", ns),
        (36, 37, "separator", "enum", '"_"'),
        (37, 47, "workflowID", "str", '"order-1001"'),
    ])
    tq = bytes.fromhex("73ab11dba8314574b804b43fedd412e7") + b"orders" + b"\x01"
    synth("tq_id", "tasks_v2 / task_queues_v2 .task_queue_id (root partition, workflow type)", tq, [
        (0, 16, "namespace_id (raw UUID bytes)", "bytes", "73ab11db-a831-4574-b804-b43fedd412e7"),
        (16, 22, "task queue name", "str", '"orders"  (partition 0 keeps the bare name)'),
        (22, 23, "type byte", "enum", "0x01 = TASK_QUEUE_TYPE_WORKFLOW, no subqueue bit"),
    ])
    tq3 = bytes.fromhex("73ab11dba8314574b804b43fedd412e7") + b"/_sys/orders/3" + b"\x02"
    synth("tq_id3", "task_queue_id for partition 3, activity type", tq3, [
        (0, 16, "namespace_id (raw UUID bytes)", "bytes", "73ab11db-a831-4574-b804-b43fedd412e7"),
        (16, 30, "partition RPC name", "str", '"/_sys/orders/3"'),
        (30, 31, "type byte", "enum", "0x02 = TASK_QUEUE_TYPE_ACTIVITY"),
    ])
    body = bytes.fromhex(A["rpc_StartWorkflowExecution"]["hex"])
    fr = bytes([0, 0, 0, 0, len(body)]) + body[:27]
    synth("grpc_frame", "gRPC Length-Prefixed-Message (first 32 B of the DATA frame payload)", fr, [
        (0, 1, "Compressed-Flag", "enum", "0x00 = not compressed"),
        (1, 5, "Message-Length (BE u32)", "len", "0x00000096 = 150"),
        (5, 14, "#1 namespace", "str", '0a 07 "default"'),
        (14, 26, "#2 workflow_id", "str", '12 0a "order-1001"'),
        (26, 32, "#3 workflow_type …", "raw", "1a 0f 0a 0d \"Or…  (continues)"),
    ])


DUMPN = [0]


def dump(key, caption, opts=""):
    d = A[key]
    b = bytes.fromhex(d["hex"])
    roles, owner, leaves = classify(d["tree"])
    maxrows = None
    m = re.search(r"rows=(\d+)", opts)
    if m:
        maxrows = int(m.group(1))
    tabledepth = 99
    m = re.search(r"depth=(\d+)", opts)
    if m:
        tabledepth = int(m.group(1))
    fid = re.sub(r"[^a-z0-9]", "", key.lower())
    DUMPN[0] += 1
    uid = f"d{DUMPN[0]}"
    rows = []
    nrows = (len(b) + 15) // 16
    for r in range(nrows):
        if maxrows and r >= maxrows and r < nrows - 1:
            if r == maxrows:
                rows.append(f'<div class="hx-row hx-gap"><span class="hx-off">…</span><span class="hx-bytes">{nrows - maxrows - 1} rows elided (full bytes in data/annot.json)</span></div>')
            continue
        cells, asc = [], []
        for c in range(16):
            i = r * 16 + c
            if i >= len(b):
                cells.append('<i class="hb pad">  </i>')
                asc.append('<i class="ha pad"> </i>')
                continue
            role = roles.get(i, "raw")
            li = owner.get(i, -1)
            ch = chr(b[i]) if 32 <= b[i] < 127 else "·"
            cells.append(f'<i class="hb r-{role}" data-l="{uid}-{li}">{b[i]:02x}</i>')
            asc.append(f'<i class="ha r-{role}" data-l="{uid}-{li}">{esc(ch)}</i>')
        rows.append(f'<div class="hx-row"><span class="hx-off">{r*16:04x}</span><span class="hx-bytes">{"".join(cells)}</span><span class="hx-asc">{"".join(asc)}</span></div>')
    trs = []
    for li, L in enumerate(leaves):
        deep = L["depth"] > tabledepth
        ind = "&nbsp;&nbsp;" * L["depth"]
        v = L["v"]
        if len(v) > 72:
            v = v[:69] + "…"
        cls = ("hdr" if L["hdr"] else "") + (" dx" if deep else "")
        trs.append(
            f'<tr class="{cls}" data-l="{uid}-{li}"><td class="mono">{L["o"]:04x}–{L["e"]-1:04x}</td>'
            f'<td class="mono path">{ind}<span class="k-{L["kind"]}">■</span> {esc(L["path"].split(".")[-1])}</td>'
            f'<td class="mono c">{L["n"]}</td><td class="mono c">{WIRE.get(L["w"],"—")}</td>'
            f'<td class="mono ty">{esc(short_type(L["t"]))}</td><td class="mono val">{esc(v)}</td></tr>'
        )
    msg = d["msg"]
    nh = sum(1 for L in leaves if L["depth"] > tabledepth)
    hidden_note = f" ({nh} deeper fields hidden in table; hover bytes to trace them)" if nh else ""
    return f'''<figure class="fig dump" data-cap="{esc(caption)}" id="fig-{fid}">
<div class="dump-head"><span class="dump-msg">{esc(msg)}</span><span class="dump-size">{len(b)} B · {len(leaves)} fields</span></div>
<div class="hx" data-uid="{uid}">{"".join(rows)}</div>
<div class="hx-status" id="st-{uid}">hover a byte or a row to trace the field</div>
<details class="dump-tbl" {"open" if len(leaves) <= 40 else ""}><summary>Field decode — {len(leaves)} fields{hidden_note} (descriptor-driven, <span class="mono">{esc(msg)}</span>)</summary>
<div class="tblwrap"><table class="ftab"><thead><tr><th>bytes</th><th>field</th><th>#</th><th>wire</th><th>type</th><th>decoded value</th></tr></thead><tbody>{"".join(trs)}</tbody></table></div></details>
<figcaption>{caption}</figcaption></figure>'''


PINS = {
    "T": ("temporalio/temporal", "d8f9c6d86b2cd0ea5c27d0694a598da84c6d337d", "temporal"),
    "API": ("temporalio/api", "1c27468c756fc8abc800235c33b74c6eba638c88", "api"),
    "SDK": ("temporalio/sdk-go", "626130f1fd9de50cfd90b884a3fb796504f22dc1", "sdk-go"),
}


def srclink(m):
    repo, sha, short = PINS[m.group(1)]
    path, a, b = m.group(2), m.group(3), m.group(4)
    frag, lab = "", f"{short}/{path}"
    if a:
        frag = f"#L{a}" + (f"-L{b}" if b else "")
        lab += f":{a}" + (f"–{b}" if b else "")
    return f'<a class="src-l" href="https://github.com/{repo}/blob/{sha}/{path}{frag}">{esc(lab)}</a>'


def timeline():
    evs = []
    for k in sorted(x for x in A if x.startswith("event_")):
        t = {n["f"]: n for n in A[k]["tree"]}
        ts = {n["f"]: int(n["v"]) for n in t["event_time"].get("k", [])}
        sec = ts.get("seconds", 0) + ts.get("nanos", 0) / 1e9
        et = t["event_type"]["v"].split("(")[1].rstrip(")").replace("EVENT_TYPE_", "")
        attrs = [n for n in A[k]["tree"] if n["f"].endswith("_attributes")][0]
        evs.append((int(t["event_id"]["v"]), et, sec, int(t["task_id"]["v"]), len(A[k]["hex"]) // 2, attrs["n"]))
    node = {}
    for k in sorted(x for x in A if x.startswith("hnode_")):
        nid = int(k.split("_")[1])
        for e in A[k]["tree"]:
            node[int(e["k"][0]["v"])] = (nid, len(A[k]["hex"]) // 2)
    t0 = evs[0][2]
    rows = []
    prev = None
    for eid, et, sec, tid, size, an in evs:
        nid, nsize = node[eid]
        first = nid != prev
        prev = nid
        bcls = ' class="batch"' if first else ""
        bar = min(100, (sec - t0) * 1000 / 1070 * 100)
        rows.append(
            f'<tr{bcls}><td class="num">{eid}</td><td class="mono">{et}</td><td class="num">+{(sec-t0)*1000:.1f}</td>'
            f'<td><div style="height:10px;border-left:1px solid var(--rule)"><div style="height:10px;width:5px;background:var(--accent);margin-left:calc({bar:.1f}% - 3px)"></div></div></td>'
            f'<td class="num">{tid}</td><td class="num">{size}</td><td class="num">#{an}</td><td class="num">{nid if first else "〃"}</td><td class="num">{nsize if first else ""}</td></tr>'
        )
    return ('<div class="tblwrap"><table class="ref compact timeline"><thead><tr><th>id</th><th>event_type</th><th>t (ms)</th><th style="min-width:120px">timeline (0–1070 ms)</th>'
            '<th>task_id</th><th>bytes</th><th>attr #</th><th>node_id</th><th>batch B</th></tr></thead><tbody>' + "".join(rows) + "</tbody></table></div>")


EVT = json.load(open(os.path.join(ROOT, "data", "event_types.json")))
API_SHA = PINS["API"][1]


def fam(name):
    for p, c in (("WORKFLOW_TASK", "var(--r-tag)"), ("ACTIVITY", "var(--r-int)"), ("TIMER", "var(--r-enum)"),
                 ("CHILD", "var(--r-len)"), ("START_CHILD", "var(--r-len)"), ("NEXUS", "var(--r-bytes)"),
                 ("WORKFLOW_EXECUTION_UPDATE", "var(--accent)")):
        if name.startswith(p):
            return c
    return "var(--ink)"


def evtplot():
    W, H, L, B = 960, 400, 50, 360
    xs = lambda n: L + n * (W - L - 20) / 61
    ys = lambda f: B - (f - 5) * (B - 20) / 61
    g = [f'<line class="dim" x1="{L}" y1="{B}" x2="{W-10}" y2="{B}"/><line class="dim" x1="{L}" y1="{B}" x2="{L}" y2="10"/>',
         f'<line class="ln2" x1="{xs(0)}" y1="{ys(5)}" x2="{xs(60)}" y2="{ys(65)}"/>',
         f'<text x="{xs(46)}" y="{ys(55)-14}" font-size="10" class="muted">field = type + 5</text>']
    for t in range(0, 61, 10):
        g.append(f'<text x="{xs(t)-6}" y="{B+16}" font-size="10" class="muted">{t}</text>')
    for f in range(5, 66, 10):
        g.append(f'<text x="{L-26}" y="{ys(f)+4}" font-size="10" class="muted">{f}</text>')
    g.append(f'<text x="{W/2-80}" y="{H-4}" font-size="11">EventType enum value →</text>')
    g.append(f'<text x="8" y="14" font-size="11">oneof field #</text>')
    for e in EVT:
        if not e.get("field"):
            continue
        off = e["field"] - e["num"] != 5
        r = 5 if off else 3.5
        g.append(f'<circle cx="{xs(e["num"]):.1f}" cy="{ys(e["field"]):.1f}" r="{r}" style="fill:{fam(e["name"])};opacity:{1 if off else .55}"><title>{e["name"]} = {e["num"]} → attributes #{e["field"]}</title></circle>')
    for n, lab, dx, dy in ((15, "ACTIVITY_TASK_CANCEL_REQUESTED 15→22", -230, -40), (17, "TIMER_STARTED 17→20", 10, 34), (20, "WF_CANCEL_REQUESTED 20→28", -60, -46), (25, "MARKER/SIGNALED/TERMINATED 25–27 → 25–27", 30, 44), (28, "CONTINUED_AS_NEW 28→33", 16, 30), (47, "UPDATE_ADMITTED 47→52", -170, -26), (41, "UPDATE_ACCEPTED 41→46", 10, 34)):
        e = [x for x in EVT if x["num"] == n][0]
        x0, y0 = xs(n), ys(e["field"])
        g.append(f'<line class="dim" x1="{x0:.0f}" y1="{y0:.0f}" x2="{x0+dx+ (0 if dx<0 else 0):.0f}" y2="{y0+dy-4:.0f}"/>')
        g.append(f'<text x="{x0+dx:.0f}" y="{y0+dy+ (8 if dy>0 else -6):.0f}" font-size="10">{lab}</text>')
    return f'<svg viewBox="0 0 {W} {H}" role="img" aria-label="Event type vs attribute field number">{"".join(g)}</svg>'


def evttable():
    rows = []
    for e in EVT:
        if e["num"] == 0:
            continue
        d = e["field"] - e["num"]
        c = "" if d == 5 else ' style="color:var(--accent);font-weight:600"'
        camel = "".join(w.capitalize() for w in e["name"].split("_"))
        ml = e.get("mline")
        link = f'<a class="src-l" href="https://github.com/temporalio/api/blob/{API_SHA}/temporal/api/history/v1/message.proto#L{ml}">{camel}EventAttributes</a>' if ml else camel
        doc = e["doc"]
        if len(doc) > 110:
            doc = doc[:107] + "…"
        tag = (e["field"] << 3) | 2
        tb = bytes([tag]) if tag < 128 else bytes([(tag & 0x7F) | 0x80, tag >> 7])
        rows.append(f'<tr><td class="num">{e["num"]}</td><td class="mono nw" style="color:{fam(e["name"])}">{e["name"]}</td>'
                    f'<td class="num"{c}>{e["field"]}</td><td class="mono nw">{tb.hex(" ")}</td><td class="nw">{link}</td><td style="font-size:12px;color:var(--ink2)">{esc(doc)}</td></tr>')
    return ('<table class="ref compact"><thead><tr><th>value</th><th>EVENT_TYPE_…</th><th>attr #</th><th>tag bytes</th><th>attributes message</th><th>proto doc comment (abridged)</th></tr></thead><tbody>'
            + "".join(rows) + "</tbody></table>")


STRUCTS = json.load(open(os.path.join(ROOT, "data", "structs.json")))
SRCMAP = {"temporal/api/": ("API", "temporal/api/"), "temporal/server/api/": ("T", "proto/internal/temporal/server/api/")}


def filelink(path):
    for pre, (repo, base) in SRCMAP.items():
        if path.startswith(pre):
            r, sha, short = PINS[repo]
            real = base + path[len(pre):]
            return f'<a class="src-l" href="https://github.com/{r}/blob/{sha}/{real}">{short}/{real}</a>'
    return esc(path)


def structs_html():
    out = []
    for m in STRUCTS["messages"]:
        rows = []
        for f in m["fields"]:
            ty = f["type"]
            scalar_varint = ty in ("int32", "int64", "uint32", "uint64", "sint32", "sint64", "bool") or ".enums." in ty
            if ty in ("double", "fixed64", "sfixed64"):
                wt = 1
            elif ty in ("float", "fixed32", "sfixed32"):
                wt = 5
            elif scalar_varint:
                wt = 2 if f["label"] else 0  # repeated scalars are packed in proto3
            else:
                wt = 2
            tag = (f["n"] << 3) | wt
            tb = []
            while True:
                b = tag & 0x7F
                tag >>= 7
                tb.append(b | (0x80 if tag else 0))
                if not tag:
                    break
            t = f["type"].replace("temporal.api.", "").replace("temporal.server.api.", "srv.").replace("google.protobuf.", "")
            rows.append(f'<tr><td class="num">{f["n"]}</td><td class="mono nw">{esc(f["name"])}{" <span class=pill>deprecated</span>" if f.get("deprecated") else ""}</td>'
                        f'<td class="mono">{esc(f["label"] + " " if f["label"] else "")}{esc(t)}</td><td class="mono nw">{bytes(tb).hex(" ")}</td><td class="mono">{esc(f.get("oneof", ""))}</td></tr>')
        res = ""
        if m.get("reserved"):
            res = '<div style="font-size:12px;color:var(--muted)">reserved: ' + ", ".join(str(a) if a == b else f"{a}–{b}" for a, b in m["reserved"]) + "</div>"
        short = m["name"].replace("temporal.api.", "").replace("temporal.server.api.", "srv.")
        out.append(f'<h4 class="sth" id="st-{m["name"].split(".")[-1].lower()}">{esc(short)}</h4><div style="font-size:12px">{filelink(m["file"])}</div>{res}'
                   f'<table class="ref compact st"><colgroup><col style="width:52px"><col style="width:32%"><col><col style="width:70px"><col style="width:12%"></colgroup><thead><tr><th>#</th><th>field</th><th>type</th><th>tag</th><th>oneof</th></tr></thead><tbody>{"".join(rows)}</tbody></table>')
    return "".join(out)


def enums_html():
    out = []
    for e in STRUCTS["enums"]:
        vals = " ".join(f'<span class="ev"><b>{v["n"]}</b> {esc(v["name"])}</span>' for v in e["values"])
        short = e["name"].replace("temporal.api.", "").replace("temporal.server.api.", "srv.")
        out.append(f'<h4 class="sth">{esc(short)} <span class="pill">{len(e["values"]) - 1} values</span></h4><div style="font-size:12px">{filelink(e["file"])}</div><div class="evs">{vals}</div>')
    return "".join(out)


def build():
    add_synthetic()
    cdir = os.path.join(ROOT, "tools", "content")
    src = "".join(open(os.path.join(cdir, f), encoding="utf-8").read() for f in sorted(os.listdir(cdir)) if f.endswith(".html"))
    src = src.replace("<!--TIMELINE-->", timeline())
    src = src.replace("<!--EVTPLOT-->", evtplot())
    src = src.replace("<!--EVTTABLE-->", evttable())
    src = src.replace("<!--STRUCTS-->", structs_html())
    src = src.replace("<!--ENUMS-->", enums_html())
    src = re.sub(r"\{\{(T|API|SDK):([^#}]+)(?:#L(\d+)(?:-L?(\d+))?)?\}\}", srclink, src)
    src = re.sub(r"<!--DUMP ([^|]+)\|(.+?)(?:\|([^|]*?))?-->", lambda m: dump(m.group(1).strip(), m.group(2).strip(), m.group(3) or ""), src, flags=re.S)
    # wrap reference tables for horizontal scroll on narrow screens
    src = re.sub(r'(<div class="tblwrap">)?(<table class="ref.*?</table>)', lambda m: m.group(0) if m.group(1) else f'<div class="tblwrap">{m.group(2)}</div>', src, flags=re.S)
    # number figures
    figs = []
    def numfig(m):
        attrs = m.group(1)
        cap = re.search(r'data-cap="([^"]*)"', attrs)
        idm = re.search(r'id="([^"]*)"', attrs)
        n = len(figs) + 1
        fid = idm.group(1) if idm else f"fig-{n}"
        figs.append((n, fid, html.unescape(cap.group(1)) if cap else ""))
        if not idm:
            attrs += f' id="{fid}"'
        return f'<figure{attrs} data-n="{n}">'
    src = re.sub(r"<figure((?:(?!>).)*class=\"fig[^\"]*\"(?:(?!>).)*)>", numfig, src)
    # figcaption prefix
    counter = iter(range(1, 10000))
    def capfix(m):
        return f'<figcaption><b class="fign">FIG. {next(counter):02d}</b> '
    src = re.sub(r"<figcaption>", capfix, src)
    # figure references [[fig:id]]
    fmap = {fid: n for n, fid, _ in figs}
    src = re.sub(r"\[\[fig:([a-z0-9\-]+)\]\]", lambda m: f'<a class="xref" href="#{m.group(1)}">Fig. {fmap.get(m.group(1), "??"):02d}</a>' if m.group(1) in fmap else f"Fig. ??({m.group(1)})", src)
    # figure index
    fi = "".join(f'<tr><td class="mono">FIG. {n:02d}</td><td><a href="#{fid}">{esc(re.sub("<[^>]+>", "", c))}</a></td></tr>' for n, fid, c in figs)
    src = src.replace("<!--FIGINDEX-->", f'<table class="ref"><thead><tr><th>No.</th><th>Figure</th></tr></thead><tbody>{fi}</tbody></table>')
    # TOC from h2/h3
    toc = []
    for m in re.finditer(r'<h([23]) id="([^"]+)"[^>]*>(.*?)</h\1>', src, flags=re.S):
        lvl, hid, txt = m.group(1), m.group(2), html.unescape(re.sub("<[^>]+>", "", m.group(3)))
        toc.append((lvl, hid, txt.strip()))
    side = []
    for lvl, hid, txt in toc:
        side.append(f'<a class="t{lvl}" href="#{hid}">{esc(txt)}</a>')
    src = src.replace("<!--SIDETOC-->", "".join(side))
    full = []
    for lvl, hid, txt in toc:
        m = re.match(r"^([A-Z0-9]+(?:\.[0-9]+)*)\s+(.*)$", txt)
        num, t = (m.group(1), m.group(2)) if m else ("", txt)
        full.append(f'<li class="l{lvl}"><a href="#{hid}"><span class="tn">{esc(num)}</span><span class="tt">{esc(t)}</span><span class="dots"></span></a></li>')
    src = src.replace("<!--FULLTOC-->", f'<ol class="toc">{"".join(full)}</ol>')
    out = os.path.join(ROOT, "temporal-technical-manual.html")
    open(out, "w", encoding="utf-8").write(src)
    print(out, len(src), "bytes;", len(figs), "figures;", len(toc), "headings")
    missing = re.findall(r"Fig\. \?\?\([^)]*\)", src) + re.findall(r"\[\[[^\]]*\]\]", src) + re.findall(r"<!--(?:DUMP|TIMELINE|EVT|STRUCTS|ENUMS|FIGINDEX)[^>]*-->", src)
    if missing:
        print("UNRESOLVED:", missing)


if __name__ == "__main__":
    build()
