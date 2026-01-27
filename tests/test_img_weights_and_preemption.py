import json
import os
import shutil
import time


def load_config():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(os.path.dirname(base_dir), "config.json")
    local_path = os.path.join(os.path.dirname(base_dir), "local_config.json")
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    if os.path.exists(local_path):
        with open(local_path, "r", encoding="utf-8") as f:
            local = json.load(f)
        cfg.update(local)
    return cfg


def clear_dir(path):
    if not os.path.exists(path):
        return
    for name in os.listdir(path):
        full = os.path.join(path, name)
        try:
            if os.path.isdir(full):
                shutil.rmtree(full)
            else:
                os.remove(full)
        except OSError:
            pass


def wait_for(condition_fn, timeout=30, interval=1):
    start = time.time()
    while time.time() - start < timeout:
        if condition_fn():
            return True
        time.sleep(interval)
    return False


def read_heartbeat(hb_dir, worker_id):
    hb_path = os.path.join(hb_dir, f"{worker_id}.json")
    try:
        with open(hb_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}


def main():
    cfg = load_config()
    root = os.path.abspath(os.path.expanduser(cfg.get("syncthing_root", "~/RenderFleet")))
    worker_id = cfg.get("worker_id", "worker001")
    img_queue = os.path.join(root, "01_job_factory", "img_queue")
    active_inbox = os.path.join(root, "02_active_floor", worker_id, "inbox")
    hb_dir = os.path.abspath(os.path.expanduser(cfg.get("heartbeat_path", os.path.join(root, "_system", "heartbeats"))))
    review_root = os.path.join(root, "03_review_room")
    commands_dir = os.path.join(root, "_system", "commands")

    os.makedirs(img_queue, exist_ok=True)
    os.makedirs(active_inbox, exist_ok=True)
    os.makedirs(commands_dir, exist_ok=True)

    # Clean room
    clear_dir(img_queue)
    clear_dir(active_inbox)

    # Step 1: Weighted Distribution Check
    for i in range(5):
        with open(os.path.join(img_queue, f"background_{i}.txt"), "w", encoding="utf-8") as f:
            for p in range(8):
                f.write(f"background {i}-{p}\n")
    for i in range(4):
        with open(os.path.join(img_queue, f"default_{i}.txt"), "w", encoding="utf-8") as f:
            for p in range(3):
                f.write(f"default {i}-{p}\n")
    with open(os.path.join(img_queue, "high_prio_0.txt"), "w", encoding="utf-8") as f:
        for p in range(2):
            f.write(f"high {p}\n")

    # Wait for dispatch, ensure high_prio is picked early
    def inbox_has_job(prefix):
        return any(name.startswith(prefix) for name in os.listdir(active_inbox))

    got_high = wait_for(lambda: inbox_has_job("high_prio"), timeout=40)
    assert got_high, "[FAIL] high_prio job was not dispatched early"

    # Step 2: VIP Preemption Check
    # Ensure background job is running
    def background_running():
        hb = read_heartbeat(hb_dir, worker_id)
        return "background" in str(hb.get("current_job", ""))

    wait_for(background_running, timeout=40)

    with open(os.path.join(img_queue, "VIP_urgent.txt"), "w", encoding="utf-8") as f:
        f.write("vip prompt 1\n")
        f.write("vip prompt 2\n")

    def yield_cmd_exists():
        return os.path.exists(os.path.join(commands_dir, f"{worker_id}.cmd"))

    yielded = wait_for(yield_cmd_exists, timeout=30)
    assert yielded, "[FAIL] Yield command was not issued within 30s"

    def background_returned():
        return any(name.startswith("background") for name in os.listdir(img_queue))

    returned = wait_for(background_returned, timeout=30)
    assert returned, "[FAIL] Background job was not returned to img_queue"

    def vip_in_inbox():
        return any("VIP_urgent" in name for name in os.listdir(active_inbox))

    vip_dispatched = wait_for(vip_in_inbox, timeout=30)
    assert vip_dispatched, "[FAIL] VIP job was not dispatched after yield"

    # Step 3: Resume Logic Check
    # Record progress count
    bg_name = None
    for name in os.listdir(img_queue):
        if name.startswith("background"):
            bg_name = os.path.splitext(name)[0]
            break

    assert bg_name, "[FAIL] No background job in queue to verify resume"

    progress_path = os.path.join(review_root, bg_name, "progress.json")
    pre_count = 0
    try:
        with open(progress_path, "r", encoding="utf-8") as f:
            pre = json.load(f)
            pre_count = len(pre.get("completed_files", []))
    except (OSError, json.JSONDecodeError):
        pre_count = 0

    # Wait for re-dispatch and progress to grow
    wait_for(lambda: inbox_has_job(bg_name), timeout=60)

    def progress_grew():
        try:
            with open(progress_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                return len(data.get("completed_files", [])) >= pre_count
        except (OSError, json.JSONDecodeError):
            return False

    assert wait_for(progress_grew, timeout=60), "[FAIL] progress.json did not preserve completed files"

    print("[PASS] Image weights + VIP preemption + resume checks passed")


if __name__ == "__main__":
    main()
