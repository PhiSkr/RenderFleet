import json
import os
import time


GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
RESET = "\033[0m"


def log_ok(msg):
    print(f"{GREEN}{msg}{RESET}")


def log_warn(msg):
    print(f"{YELLOW}{msg}{RESET}")


def log_fail(msg):
    print(f"{RED}{msg}{RESET}")


def load_config():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(os.path.dirname(base_dir), "config.json")
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def clear_dir(path):
    try:
        entries = os.listdir(path)
    except OSError:
        return
    for name in entries:
        full = os.path.join(path, name)
        try:
            if os.path.isdir(full):
                for sub in os.listdir(full):
                    sub_path = os.path.join(full, sub)
                    if os.path.isfile(sub_path) or os.path.islink(sub_path):
                        os.remove(sub_path)
                continue
            os.remove(full)
        except OSError:
            continue


def list_heartbeats(hb_dir):
    try:
        files = [f for f in os.listdir(hb_dir) if f.endswith(".json")]
    except OSError:
        return []
    data = []
    for name in files:
        path = os.path.join(hb_dir, name)
        try:
            with open(path, "r", encoding="utf-8") as f:
                hb = json.load(f)
                data.append(hb)
        except (OSError, json.JSONDecodeError):
            continue
    return data


def main():
    config = load_config()
    syncthing_root = os.path.expanduser(config.get("syncthing_root", "~/RenderFleet"))
    heartbeat_path = config.get("heartbeat_path")
    if heartbeat_path:
        heartbeat_dir = os.path.abspath(os.path.expanduser(heartbeat_path))
    else:
        heartbeat_dir = os.path.join(syncthing_root, "_system", "heartbeats")

    img_queue = os.path.join(syncthing_root, "01_job_factory", "img_queue")
    vid_queue = os.path.join(syncthing_root, "01_job_factory", "vid_queue")
    active_floor = os.path.join(syncthing_root, "02_active_floor")
    review_room = os.path.join(syncthing_root, "03_review_room")

    os.makedirs(img_queue, exist_ok=True)
    os.makedirs(vid_queue, exist_ok=True)
    os.makedirs(active_floor, exist_ok=True)

    clear_dir(img_queue)
    clear_dir(vid_queue)
    try:
        for worker in os.listdir(active_floor):
            inbox = os.path.join(active_floor, worker, "inbox")
            clear_dir(inbox)
    except OSError:
        pass

    log_ok("Phase 1: Flooding background jobs...")
    for i in range(5):
        path = os.path.join(img_queue, f"background_job_{i}.txt")
        with open(path, "w", encoding="utf-8") as f:
            for p in range(10):
                f.write(f"background prompt {i}-{p}\n")

    time.sleep(5)
    heartbeats = list_heartbeats(heartbeat_dir)
    busy_background = [
        hb
        for hb in heartbeats
        if hb.get("status") == "BUSY" and "background_job" in str(hb.get("current_job", ""))
    ]
    if not busy_background:
        log_warn("No workers reported busy background jobs yet.")
    else:
        log_ok(f"Detected {len(busy_background)} busy background workers.")

    log_ok("Phase 2: Injecting VIP job...")
    vip_name = "VIP_urgent_job.txt"
    vip_path = os.path.join(img_queue, vip_name)
    with open(vip_path, "w", encoding="utf-8") as f:
        f.write("vip prompt 1\n")
        f.write("vip prompt 2\n")

    log_ok("Phase 3: Monitoring for preemption...")
    start_time = time.time()
    vip_started = False
    background_returned = False
    vip_completed = False

    while time.time() - start_time < 180:
        time.sleep(2)
        heartbeats = list_heartbeats(heartbeat_dir)
        for hb in heartbeats:
            current_job = str(hb.get("current_job", ""))
            if "VIP_" in current_job or "vip" in current_job.lower():
                vip_started = True

        if os.path.exists(os.path.join(img_queue, "background_job_0.txt")):
            background_returned = True

        vip_done_path = os.path.join(review_room, "VIP_urgent_job", "VIP_urgent_job.txt")
        if os.path.exists(vip_done_path):
            vip_completed = True

        if vip_started and background_returned and vip_completed:
            break

    if vip_started:
        log_ok("VIP job started on a worker.")
    else:
        log_fail("VIP job never started.")

    if background_returned:
        log_ok("Background job was returned to queue.")
    else:
        log_fail("Background job did not return to queue.")

    if vip_completed:
        log_ok("VIP job completed successfully.")
    else:
        log_fail("VIP job did not complete in time.")

    if vip_started and background_returned and vip_completed:
        log_ok("PASS: VIP preemption behavior verified.")
        return 0

    log_fail("FAIL: VIP preemption behavior not observed.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
