import json
import os
import shutil
import time

from dispatcher import FleetDispatcher


def load_settings(root):
    settings_path = os.path.join(root, "_system", "settings.json")
    with open(settings_path, "r", encoding="utf-8") as f:
        return json.load(f)


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


def make_jobs_img(queue, key, count=5):
    for i in range(count):
        path = os.path.join(queue, f"{key}_img_job_{i}.txt")
        with open(path, "w", encoding="utf-8") as f:
            f.write("prompt\n")


def make_jobs_vid(queue, key, count=5):
    for i in range(count):
        job_dir = os.path.join(queue, f"{key}_vid_job_{i}")
        os.makedirs(job_dir, exist_ok=True)
        for j in range(2):
            frame = os.path.join(job_dir, f"frame_{j:03d}.png")
            with open(frame, "w", encoding="utf-8") as f:
                f.write("data")


def detect_key(filename, weights):
    lower = filename.lower()
    best_key = None
    best_weight = None
    for key, weight in weights.items():
        if key == "default":
            continue
        if key.lower() in lower:
            if best_weight is None or weight > best_weight:
                best_key = key
                best_weight = weight
    if best_key is None:
        return "default", int(weights.get("default", 1))
    return best_key, int(best_weight)


def simulate_selection(dispatcher, queue, weights, iterations=200):
    counts = {}
    for _ in range(iterations):
        job = dispatcher.get_next_job(queue, weights)
        if not job:
            break
        key, _weight = detect_key(os.path.basename(job), weights)
        counts[key] = counts.get(key, 0) + 1
    return counts


def main():
    root = os.path.expanduser("~/RenderFleet")
    settings = load_settings(root)
    weights = settings.get("weights", {})

    img_queue = os.path.join(root, "01_job_factory", "img_queue")
    vid_queue = os.path.join(root, "01_job_factory", "vid_queue")

    os.makedirs(img_queue, exist_ok=True)
    os.makedirs(vid_queue, exist_ok=True)

    clear_dir(img_queue)
    clear_dir(vid_queue)

    for key in weights.keys():
        make_jobs_img(img_queue, key)
        make_jobs_vid(vid_queue, key)

    dispatcher = FleetDispatcher({"syncthing_root": root, "weights": weights}, lambda p: os.path.join(root, p))

    # Validation A: Matching
    bg_key = "background"
    if bg_key in weights:
        expected = int(weights[bg_key])
        assert expected == 6, f"[FAIL] background weight expected 6 but got {expected}"
        sample = "background_img_job_1.txt"
        matched_key, matched_weight = detect_key(sample, weights)
        assert matched_key == bg_key, f"[FAIL] {sample} did not match background"
        assert matched_weight == expected, f"[FAIL] {sample} matched weight {matched_weight}"
        print("[PASS] Validation A: background matching & weight ok")
    else:
        raise AssertionError("[FAIL] background key not found in settings.json")

    # Validation B: DRR Math
    if "default" in weights and "test" in weights:
        counts = simulate_selection(dispatcher, img_queue, weights, iterations=200)
        default_count = counts.get("default", 0)
        test_count = counts.get("test", 0)
        assert default_count > test_count * 5, (
            f"[FAIL] Expected default to dominate test. default={default_count}, test={test_count}"
        )
        print("[PASS] Validation B: default dominates test")
    else:
        print("[WARN] Validation B skipped (default/test missing)")

    # Validation C: Isolation
    dispatcher_img = FleetDispatcher({"syncthing_root": root, "weights": weights}, lambda p: os.path.join(root, p))
    dispatcher_vid = FleetDispatcher({"syncthing_root": root, "weights": weights}, lambda p: os.path.join(root, p))

    # Warm img dispatcher with many selections
    simulate_selection(dispatcher_img, img_queue, weights, iterations=50)

    # Compare vid selection on fresh dispatcher vs warmed shared dispatcher
    fresh_vid_counts = simulate_selection(dispatcher_vid, vid_queue, weights, iterations=50)
    shared_vid_counts = simulate_selection(dispatcher_img, vid_queue, weights, iterations=50)

    if fresh_vid_counts == shared_vid_counts:
        print("PASSED: Image/Video DRR state is isolated")
    else:
        print("FAILED: DRR state is shared")

    # Summary table
    print("\n[SUMMARY]")
    for key, weight in weights.items():
        count = simulate_selection(dispatcher, img_queue, weights, iterations=50).get(key, 0)
        print(f"{key} -> {weight} -> {count}")


if __name__ == "__main__":
    main()
