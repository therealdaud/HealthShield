import os, json, time, math, decimal, boto3
from urllib.parse import parse_qs
from datetime import datetime

TABLE_NAME = os.environ["TABLE_NAME"]
SITE_ID    = os.environ.get("SITE_ID", "tampa_usf_valet")
SNS_ARN    = os.environ.get("SNS_TOPIC_ARN", "")

dynamodb = boto3.resource("dynamodb")
table    = dynamodb.Table(TABLE_NAME)
sns      = boto3.client("sns") if SNS_ARN else None

# ----------------------- basic HI -----------------------
def c_to_f(c): return (c * 9.0/5.0) + 32.0

def heat_index_f(T_f, RH):
    T = T_f; R = RH
    return (-42.379 + 2.04901523*T + 10.14333127*R
            - 0.22475541*T*R - 6.83783e-3*T*T - 5.481717e-2*R*R
            + 1.22874e-3*T*T*R + 8.5282e-4*T*R*R - 1.99e-6*T*T*R*R)

def bucket_from_hi(h):
    if h >= 125: return "Red"
    if h >= 103: return "Orange"
    if h >= 90:  return "Yellow"
    return "Green"

def next_break_eta(bucket):
    if bucket == "Red":    return 0
    if bucket == "Orange": return 12
    return 30

# ----------------------- persistence -----------------------
def put_reading(site_id, device_id, ts, temp_c, rh, hi_f, hi_eff_f):
    ttl = int(time.time()) + 7*24*3600
    item = {
        "pk": f"SENSOR#{site_id}#{device_id}",
        "sk": f"TS#{ts}",
        "ts": ts,
        "temp_c": decimal.Decimal(str(temp_c)),
        "rh_pct": decimal.Decimal(str(rh)),
        "hi_f": decimal.Decimal(str(hi_f)),
        "hi_eff_f": decimal.Decimal(str(hi_eff_f)),
        "ttl": ttl
    }
    table.put_item(Item=item)

def get_user_state(site_id, user_id):
    res = table.get_item(Key={"pk": f"STATE#{site_id}", "sk": f"USER#{user_id}"})
    return res.get("Item")

def put_user_state(site_id, user_id, state):
    item = {"pk": f"STATE#{site_id}", "sk": f"USER#{user_id}", **state}
    table.put_item(Item=item)

def get_user_profile(site_id, user_id):
    res = table.get_item(Key={"pk": f"PROFILE#{site_id}", "sk": f"USER#{user_id}"})
    return res.get("Item") or default_profile(user_id)

def put_user_profile(site_id, user_id, prof):
    item = {"pk": f"PROFILE#{site_id}", "sk": f"USER#{user_id}", **prof}
    table.put_item(Item=item)

# ----------------------- personalization -----------------------
def default_profile(user_id="demo_user"):
    # Sensible defaults (units in °F deltas)
    return {
        "user_id": user_id,
        "exertion_default": 2,     # 1..5
        "acclim_days": 0,          # 0..14
        "clothing": "normal",      # light|normal|heavy
        "wind_mps": 1.0,           # if you don't fetch wind, set a site default
        "coeff": {                 # tunable “HI+” weights
            "k_solar": 6.0,        # midday sun adds up to ~6°F
            "k_wind": 4.0,         # 4 m/s wind cools ~4°F
            "k_exertion": 3.0,     # from level 2 baseline to level 5
            "k_acclim": -2.0,      # fully acclimated (-2°F by 14 days)
            "k_clothing": 2.0      # heavy adds ~2°F; normal ~1.2; light ~0
        }
    }

def solar_intensity_factor(ts_local: int, in_shade: bool):
    """
    Very light proxy for solar load: triangular shape peaking ~1pm.
    Returns 0..1, reduced strongly if in shade.
    """
    hour = datetime.fromtimestamp(ts_local).hour  # uses Lambda's local tz (UTC by default)
    # If your Lambda runs UTC, adjust to local (e.g., Tampa UTC-4):
    hour = (hour - 4) % 24
    peak = 13
    width = 6  # ~10am-4pm active window
    val = max(0.0, 1.0 - abs(hour - peak)/ (width/2))
    if in_shade:
        val *= 0.3
    return min(1.0, max(0.0, val))

def clothing_factor(clothing: str):
    return {"light": 0.0, "normal": 0.6, "heavy": 1.0}.get(clothing, 0.6)

def personalized_hi(hi_base_f: float, ts: int, profile: dict, state: dict):
    c = profile.get("coeff", {})
    k_solar    = float(c.get("k_solar", 6.0))
    k_wind     = float(c.get("k_wind", 4.0))
    k_exertion = float(c.get("k_exertion", 3.0))
    k_acclim   = float(c.get("k_acclim", -2.0))
    k_clo      = float(c.get("k_clothing", 2.0))

    in_shade = bool(state.get("in_shade", False))
    exertion = int(state.get("exertion_level", profile.get("exertion_default", 2)))
    acclim   = int(profile.get("acclim_days", 0))
    wind     = float(profile.get("wind_mps", 1.0))
    clo_idx  = clothing_factor(profile.get("clothing","normal"))

    # 0..1 solar factor, reduced if shaded
    solar = solar_intensity_factor(ts, in_shade)

    # Normalize pieces to 0..1 and combine as °F delta
    wind_norm     = min(wind, 4.0) / 4.0                  # cap 4 m/s
    exertion_norm = (max(1, min(exertion,5)) - 2) / 3.0   # -0.33..1.0 from baseline 2
    acclim_norm   = min(max(acclim, 0), 14) / 14.0        # 0..1
    clo_norm      = clo_idx                               # 0.0 light, 0.6 normal, 1.0 heavy

    delta_f = (
        k_solar * solar
        - k_wind * wind_norm
        + k_exertion * exertion_norm
        + k_acclim * acclim_norm
        + k_clo * clo_norm
    )
    return hi_base_f + delta_f

# ----------------------- nudges -----------------------
def maybe_send_nudge(user_id, new_bucket, state_now):
    if not sns: return
    if new_bucket not in ("Orange","Red"): return
    now = int(time.time())
    last_bucket = state_now.get("last_bucket")
    last_nudge  = int(state_now.get("last_nudge_ts", 0))
    if last_bucket == new_bucket and (now - last_nudge) < 600:
        return
    msg = "HeatShield: "
    if new_bucket == "Red":
        msg += "RED risk. Seek shade and rest now. Hydrate."
    else:
        msg += "ORANGE risk. Hydrate now; plan 5-min cool-down in ~12 min."
    try:
        sns.publish(TopicArn=SNS_ARN, Message=msg, Subject="Heat alert")
        state_now["last_nudge_ts"] = now
    except Exception as e:
        print("SNS publish failed:", e)

# ----------------------- handlers -----------------------
def handle_iot(event):
    msg = event.get("message", event)
    try:
        site_id   = msg.get("site_id", SITE_ID)
        device_id = msg["device_id"]
        ts        = int(msg.get("ts", time.time()))
        temp_c    = float(msg["temp_c"])
        rh        = float(msg["rh_pct"])
    except Exception as e:
        print("bad payload:", event, "err:", e); return {"status":"bad_payload"}

    temp_f = c_to_f(temp_c)
    hi     = heat_index_f(temp_f, rh)

    user_id = "demo_user"
    state   = get_user_state(site_id, user_id) or {}
    prof    = get_user_profile(site_id, user_id)

    hi_eff  = personalized_hi(hi, ts, prof, state)
    bucket  = bucket_from_hi(hi_eff)
    eta     = next_break_eta(bucket)

    put_reading(site_id, device_id, ts, temp_c, rh, hi, hi_eff)

    state.update({
        "updated_at": ts,
        "user_id": user_id,
        "hi_nowcast_f": decimal.Decimal(f"{hi_eff:.3f}"),
        "risk_bucket": bucket,
        "next_break_eta_min": eta,
        "source": "sensor+personalized",
        "last_bucket": bucket
    })
    maybe_send_nudge(user_id, bucket, state)
    put_user_state(site_id, user_id, state)
    return {"status":"ok"}

def http_json(body, code=200):
    return {
        "statusCode": code,
        "headers": {
            "content-type":"application/json",
            "access-control-allow-origin":"*",
            "access-control-allow-methods":"GET,POST,OPTIONS",
            "access-control-allow-headers":"*",
        },
        "body": json.dumps(body)
    }

def handle_http(event):
    method = event.get("requestContext",{}).get("http",{}).get("method","GET")
    path   = event.get("requestContext",{}).get("http",{}).get("path","/")
    raw_qs = event.get("rawQueryString","")
    qs = {k:v[0] for k,v in parse_qs(raw_qs).items()}
    user_id = qs.get("user_id","demo_user")
    site_id = qs.get("site_id", SITE_ID)

    if method == "OPTIONS":
        return http_json({"ok": True})

    if method == "GET" and path.endswith("/risk/now"):
        state = get_user_state(site_id, user_id)
        if not state:
            return http_json({"ok": True, "message": "no data yet"})
        out = {
            "ok": True,
            "site_id": site_id,
            "user_id": user_id,
            "time": state.get("updated_at"),
            "hi_nowcast_f": float(state.get("hi_nowcast_f", 0.0)),
            "bucket": state.get("risk_bucket","Green"),
            "next_break_eta_min": int(state.get("next_break_eta_min", 30)),
            "source": state.get("source","sensor")
        }
        return http_json(out)

    if method == "POST" and path.endswith("/profile"):
        # body: { user_id?, exertion_default, acclim_days, clothing, wind_mps, coeff? }
        body = json.loads(event.get("body","") or "{}")
        uid  = body.get("user_id", user_id)
        prof = get_user_profile(site_id, uid)
        prof.update({k:v for k,v in body.items() if k != "user_id"})
        put_user_profile(site_id, uid, prof)
        return http_json({"ok": True, "profile": prof})

    if method == "POST" and path.endswith("/state"):
        # body: { user_id?, in_shade?, exertion_level? }
        body = json.loads(event.get("body","") or "{}")
        uid  = body.get("user_id", user_id)
        st   = get_user_state(site_id, uid) or {}
        if "in_shade" in body: st["in_shade"] = bool(body["in_shade"])
        if "exertion_level" in body:
            lvl = int(body["exertion_level"]); lvl = max(1, min(lvl,5)); st["exertion_level"] = lvl
        put_user_state(site_id, uid, st)
        return http_json({"ok": True, "state": st})

    return http_json({"ok": False, "error": "not found"}, 404)

def handler(event, context):
    if "requestContext" in event and "http" in event["requestContext"]:
        return handle_http(event)          # Function URL
    if event.get("source") == "aws.events":
        return {"status": "noop"}
    return handle_iot(event)               # IoT ingest
