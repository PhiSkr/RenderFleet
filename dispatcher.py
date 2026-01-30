import json
import os
import random
import re
import shutil
import time


class FleetDispatcher:
    def __init__(self, config, get_sys_path, logger=print):
        self.config = config
        self.get_sys_path = get_sys_path
        self.logger = logger
        self.deficits = {}
        self.current_index = {}

    def _load_weights(self):
        root = self.config.get("syncthing_root") or "~/RenderFleet"
        root = os.path.abspath(os.path.expanduser(root))
        settings_path = os.path.join(root, "_system", "settings.json")
        weights_cfg = self.config.get("weights", {}) or {}
        if os.path.exists(settings_path):
            try:
                with open(settings_path, "r", encoding="utf-8") as f:
                    settings = json.load(f)
                if isinstance(settings, dict) and "weights" in settings:
                    weights_cfg = settings.get("weights", weights_cfg) or weights_cfg
            except (OSError, json.JSONDecodeError):
                pass
        if "default" not in weights_cfg:
            weights_cfg["default"] = 1
        return weights_cfg

    def check_dead_workers(self, heartbeat_dir, active_floor_path, job_queue_path):
        hb_files = []
        try:
            hb_files = [
                os.path.join(heartbeat_dir, f)
                for f in os.listdir(heartbeat_dir)
                if f.endswith(".json")
            ]
        except OSError:
            hb_files = []

        now = int(time.time())
        for hb_path in hb_files:
            try:
                with open(hb_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except (OSError, json.JSONDecodeError):
                continue

            if not isinstance(data, dict):
                continue

            ts = data.get("timestamp")
            status = data.get("status")
            worker_id = data.get("worker_id")
            if not worker_id or not isinstance(ts, int):
                continue

            if now - ts <= 180 or status != "BUSY":
                continue

            inbox_path = os.path.join(active_floor_path, worker_id, "inbox")
            try:
                entries = sorted(os.listdir(inbox_path))
            except OSError:
                entries = []

            for entry in entries:
                job_path = os.path.join(inbox_path, entry)
                if not (os.path.isfile(job_path) or os.path.isdir(job_path)):
                    continue
                try:
                    os.makedirs(job_queue_path, exist_ok=True)
                    shutil.move(job_path, os.path.join(job_queue_path, entry))
                    self.logger(f"Recovered job {entry} from dead worker {worker_id}")
                except OSError:
                    continue

    def _get_idle_workers(self, target_type=None, include_self_id=False):
        hb_dir = self.config.get("heartbeat_path")
        if hb_dir:
            hb_dir = os.path.abspath(os.path.expanduser(hb_dir))
        else:
            hb_dir = self.get_sys_path(os.path.join("_system", "heartbeats"))
        try:
            hb_files = [
                os.path.join(hb_dir, f)
                for f in os.listdir(hb_dir)
                if f.endswith(".json")
            ]
        except OSError:
            hb_files = []

        now = int(time.time())
        idle_workers = []
        allowed_roles = None
        if target_type == "img":
            allowed_roles = {"img_worker", "img_lead"}
        elif target_type == "vid":
            allowed_roles = {"vid_worker", "vid_lead"}

        for hb_path in hb_files:
            try:
                with open(hb_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except (OSError, json.JSONDecodeError):
                continue

            if not isinstance(data, dict):
                continue

            ts = data.get("timestamp")
            status = data.get("status")
            role = data.get("role")
            worker_id = data.get("worker_id")
            if not worker_id or not isinstance(ts, int):
                continue

            if now - ts < 90 and status == "IDLE":
                if allowed_roles is not None and role not in allowed_roles:
                    continue
                idle_workers.append(worker_id)

        if include_self_id:
            self_id = self.config.get("worker_id")
            self_role = self.config.get("initial_role")
            self_status = self.config.get("last_status")
            if (
                self_id
                and self_status == "IDLE"
                and (allowed_roles is None or self_role in allowed_roles)
            ):
                if self_id in idle_workers:
                    idle_workers.remove(self_id)
                idle_workers.insert(0, self_id)

        self.logger(
            f"DEBUG: Active idle workers within 90s window: {len(idle_workers)}"
        )
        return idle_workers

    def get_next_job(self, queue_path, config_weights):
        self.logger(f"DEBUG: Scanning queue at {queue_path}")
        try:
            entries = [
                os.path.join(queue_path, f)
                for f in os.listdir(queue_path)
                if not f.startswith(".")
            ]
        except OSError as e:
            self.logger(f"DEBUG: Failed to list queue: {e}")
            entries = []

        jobs = [p for p in entries if os.path.isfile(p) or os.path.isdir(p)]
        if not jobs:
            self.logger("DEBUG: Queue empty (0 valid jobs found).")
            return None

        for job in jobs:
            name = os.path.basename(job)
            if re.search(r"(vip|urgent)", name, re.IGNORECASE):
                self.logger(f"DEBUG: Selected VIP job: {os.path.basename(job)}")
                return job

        weights_cfg = self._load_weights()
        default_weight = int(weights_cfg.get("default", 1))
        keys = [k for k in weights_cfg.keys() if k != "default"]

        buckets = {k: [] for k in keys}
        buckets["default"] = []

        for job in jobs:
            name = os.path.basename(job)
            lower_name = name.lower()
            matched_key = None
            matched_weight = None
            for key in keys:
                if key.lower() in lower_name:
                    weight = int(weights_cfg.get(key, default_weight))
                    if matched_weight is None or weight > matched_weight:
                        matched_key = key
                        matched_weight = weight
            if matched_key:
                self.logger(
                    f"DEBUG: 🎯 Match! {name} contains '{matched_key}' -> Weight: {matched_weight}"
                )
                buckets.setdefault(matched_key, []).append(job)
            else:
                self.logger(
                    f"DEBUG: ℹ️ No keyword found in {name}, falling back to 'default' ({default_weight})"
                )
                buckets.setdefault("default", []).append(job)

        key_order = [k for k in keys] + ["default"]
        if not key_order:
            key_order = ["default"]

        queue_key = os.path.abspath(queue_path)
        if queue_key not in self.deficits:
            self.deficits[queue_key] = {}
        if queue_key not in self.current_index:
            self.current_index[queue_key] = 0

        for key in key_order:
            self.deficits[queue_key].setdefault(key, 0)

        total_keys = len(key_order)

        def _try_select_job():
            attempts = 0
            while attempts < total_keys:
                idx = self.current_index[queue_key] % total_keys
                category = key_order[idx]

                self.logger(
                    f"DEBUG: DRR State - Bucket: {category}, "
                    f"Credit: {self.deficits[queue_key][category]}, "
                    f"Total Jobs in Queue: {len(jobs)}"
                )

                if buckets.get(category) and self.deficits[queue_key][category] > 0:
                    job = buckets[category].pop(0)
                    self.deficits[queue_key][category] -= 1
                    if (
                        self.deficits[queue_key][category] == 0
                        or not buckets.get(category)
                    ):
                        self.current_index[queue_key] = (idx + 1) % total_keys
                    self.logger(f"DEBUG: Selected job: {os.path.basename(job)}")
                    return job

                self.current_index[queue_key] = (idx + 1) % total_keys
                attempts += 1

            return None

        job = _try_select_job()
        if job is not None:
            return job

        for key in key_order:
            self.deficits[queue_key][key] = max(
                0, int(weights_cfg.get(key, default_weight))
            )

        return _try_select_job()

    def enforce_vip_preemption(self, queue_path, active_floor_path):
        try:
            entries = [
                f for f in os.listdir(queue_path) if not f.startswith(".")
            ]
        except OSError:
            entries = []

        if not any("vip" in name.lower() for name in entries):
            return

        hb_dir = self.config.get("heartbeat_path")
        if hb_dir:
            hb_dir = os.path.abspath(os.path.expanduser(hb_dir))
        else:
            hb_dir = self.get_sys_path(os.path.join("_system", "heartbeats"))

        try:
            hb_files = [
                os.path.join(hb_dir, f)
                for f in os.listdir(hb_dir)
                if f.endswith(".json")
            ]
        except OSError:
            hb_files = []

        idle_found = False
        victim_worker = None
        for hb_path in hb_files:
            try:
                with open(hb_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except (OSError, json.JSONDecodeError):
                continue

            if not isinstance(data, dict):
                continue

            status = data.get("status")
            worker_id = data.get("worker_id")
            current_job = data.get("current_job", "") or ""
            if not worker_id:
                continue

            if status == "IDLE":
                idle_found = True
                break

            if status == "BUSY" and "vip" not in current_job.lower():
                victim_worker = worker_id

        if idle_found or not victim_worker:
            return

        cmd_dir = self.get_sys_path(os.path.join("_system", "commands"))
        os.makedirs(cmd_dir, exist_ok=True)
        cmd_path = os.path.join(cmd_dir, f"{victim_worker}.cmd")
        try:
            with open(cmd_path, "w", encoding="utf-8") as f:
                json.dump({"action": "yield", "reason": "vip_waiting"}, f)
            self.logger(
                f"⚠️ VIP Waiting. Commanding worker {victim_worker} to YIELD current job."
            )
        except OSError:
            return

    def dispatch_smart(self):
        role = self.config.get("initial_role")
        self.logger(f"DEBUG: Dispatching for role {role}")
        if role == "img_lead":
            source_rel = os.path.join("01_job_factory", "img_queue")
            target_type = "img"
        elif role == "vid_lead":
            source_rel = os.path.join("01_job_factory", "vid_queue")
            target_type = "vid"
        else:
            return

        self.logger(f"DEBUG: Dispatching for role {role}, looking in {source_rel}")
        source_path = self.get_sys_path(source_rel)
        idle_workers = self._get_idle_workers(
            target_type=target_type, include_self_id=True
        )
        self.logger(f"DEBUG: Found {len(idle_workers)} idle workers: {idle_workers}")
        if not idle_workers:
            return

        job_path = self.get_next_job(source_path, self.config.get("weights", {}))
        if not job_path:
            return

        filename = os.path.basename(job_path)
        worker_id = idle_workers[0]
        inbox_path = self.get_sys_path(os.path.join("02_active_floor", worker_id, "inbox"))
        os.makedirs(inbox_path, exist_ok=True)
        try:
            self.logger(f"DEBUG: Attempting to move {filename} to {inbox_path}")
            shutil.move(job_path, os.path.join(inbox_path, filename))
            self.logger(f"CMD: Dispatched {filename} to {worker_id}")
        except Exception as e:
            self.logger(f"❌ DISPATCH ERROR: Failed to move {filename}. Reason: {e}")
            return

    def recover_dead_workers(self):
        hb_dir = self.config.get("heartbeat_path")
        if hb_dir:
            hb_dir = os.path.abspath(os.path.expanduser(hb_dir))
        else:
            hb_dir = self.get_sys_path(os.path.join("_system", "heartbeats"))
        active_floor = self.get_sys_path("02_active_floor")
        img_queue = self.get_sys_path(os.path.join("01_job_factory", "img_queue"))
        vid_queue = self.get_sys_path(os.path.join("01_job_factory", "vid_queue"))
        self.check_dead_workers(hb_dir, active_floor, img_queue)
        self.check_dead_workers(hb_dir, active_floor, vid_queue)
