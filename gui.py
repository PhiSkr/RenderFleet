import customtkinter as ctk
import os
import json
import time
import glob
from datetime import datetime
import shutil
import re

ctk.set_appearance_mode("Dark")
ctk.set_default_color_theme("dark-blue")


class WorkerCard(ctk.CTkFrame):
    def __init__(self, master, worker_data, is_online):
        status_text, border_color = self._derive_status(worker_data, is_online)

        super().__init__(master, fg_color="#1f1f1f", border_width=2, border_color=border_color, corner_radius=10)

        worker_id = worker_data.get("worker_id", "Unknown")
        role = worker_data.get("role", "Unknown")
        current_job = worker_data.get("current_job", "None")
        last_seen = worker_data.get("last_seen", "Unknown")

        self.worker_id = worker_id

        self.title_label = ctk.CTkLabel(self, text=f"Worker ID: {worker_id}", font=("Helvetica", 16, "bold"))
        self.title_label.pack(anchor="w", padx=12, pady=(10, 2))

        self.status_label = ctk.CTkLabel(self, text=f"Status: {status_text}")
        self.status_label.pack(anchor="w", padx=12)

        self.role_label = ctk.CTkLabel(self, text=f"Role: {role}")
        self.role_label.pack(anchor="w", padx=12)

        self.job_label = ctk.CTkLabel(self, text=f"Current Job: {current_job}")
        self.job_label.pack(anchor="w", padx=12)

        self.last_seen_label = ctk.CTkLabel(self, text=f"Last Seen: {last_seen}")
        self.last_seen_label.pack(anchor="w", padx=12, pady=(0, 6))

        time_diff = worker_data.get("time_diff", 0)
        btn_state = "normal" if is_online and time_diff <= 90 else "disabled"

        power_row = ctk.CTkFrame(self, fg_color="transparent")
        power_row.pack(fill="x", padx=12, pady=(0, 6))

        self.start_btn = ctk.CTkButton(
            power_row,
            text="▶️ START",
            width=110,
            height=26,
            fg_color="green",
            command=lambda: self.send_command(worker_id, "unpause"),
            state=btn_state,
        )
        self.start_btn.grid(row=0, column=0, padx=4, pady=4, sticky="w")

        self.stop_btn = ctk.CTkButton(
            power_row,
            text="⏸ STOP",
            width=110,
            height=26,
            fg_color="red",
            command=lambda: self.send_command(worker_id, "pause"),
            state=btn_state,
        )
        self.stop_btn.grid(row=0, column=1, padx=4, pady=4, sticky="w")

        button_row = ctk.CTkFrame(self, fg_color="transparent")
        button_row.pack(fill="x", padx=12, pady=(0, 8))

        self.img_btn = ctk.CTkButton(
            button_row,
            text="Set: ImgLead",
            width=110,
            height=26,
            command=lambda: self.send_command(worker_id, "set_role", "img_lead"),
            state=btn_state,
        )
        self.img_btn.grid(row=0, column=0, padx=4, pady=4, sticky="w")

        self.vid_btn = ctk.CTkButton(
            button_row,
            text="Set: VidLead",
            width=110,
            height=26,
            command=lambda: self.send_command(worker_id, "set_role", "vid_lead"),
            state=btn_state,
        )
        self.vid_btn.grid(row=0, column=1, padx=4, pady=4, sticky="w")

        self.img_worker_btn = ctk.CTkButton(
            button_row,
            text="Set: ImgWork",
            width=110,
            height=26,
            command=lambda: self.send_command(worker_id, "set_role", "img_worker"),
            state=btn_state,
        )
        self.img_worker_btn.grid(row=1, column=0, padx=4, pady=4, sticky="w")

        self.vid_worker_btn = ctk.CTkButton(
            button_row,
            text="Set: VidWork",
            width=110,
            height=26,
            command=lambda: self.send_command(worker_id, "set_role", "vid_worker"),
            state=btn_state,
        )
        self.vid_worker_btn.grid(row=1, column=1, padx=4, pady=4, sticky="w")

        self.feedback_label = ctk.CTkLabel(self, text="", text_color="#2ecc71")
        self.feedback_label.pack(anchor="w", padx=12, pady=(0, 10))

    def _derive_status(self, worker_data, is_online):
        status = worker_data.get("status", "Unknown")
        time_diff = worker_data.get("time_diff", 0)

        if not is_online or time_diff > 90:
            border_color = "#d9534f"  # red
            status_text = "Offline"
        else:
            if status.lower() == "busy":
                border_color = "#1f6feb"  # blue
            else:
                border_color = "#2ecc71"  # green
            status_text = status

        return status_text, border_color

    def update_state(self, worker_data, is_online):
        status_text, border_color = self._derive_status(worker_data, is_online)
        role = worker_data.get("role", "Unknown")
        current_job = worker_data.get("current_job", "None")
        last_seen = worker_data.get("last_seen", "Unknown")
        time_diff = worker_data.get("time_diff", 0)

        self.status_label.configure(text=f"Status: {status_text}")
        self.role_label.configure(text=f"Role: {role}")
        self.job_label.configure(text=f"Current Job: {current_job}")
        self.last_seen_label.configure(text=f"Last Seen: {last_seen}")
        self.configure(border_color=border_color)

        btn_state = "normal" if is_online and time_diff <= 90 else "disabled"
        self.start_btn.configure(state=btn_state)
        self.stop_btn.configure(state=btn_state)
        self.img_btn.configure(state=btn_state)
        self.vid_btn.configure(state=btn_state)
        self.img_worker_btn.configure(state=btn_state)
        self.vid_worker_btn.configure(state=btn_state)

    def send_command(self, worker_id, action, value=None):
        if not worker_id:
            return
        cmd = {"action": action}
        if value is not None:
            cmd["value"] = value
        cmd_path = os.path.expanduser(
            os.path.join("~", "RenderFleet", "_system", "commands", f"{worker_id}.cmd")
        )
        os.makedirs(os.path.dirname(cmd_path), exist_ok=True)
        try:
            with open(cmd_path, "w", encoding="utf-8") as f:
                json.dump(cmd, f)
            self._show_feedback("Sent!")
        except OSError:
            self._show_feedback("Failed", is_error=True)

    def _show_feedback(self, message, is_error=False):
        color = "#d9534f" if is_error else "#2ecc71"
        self.feedback_label.configure(text=message, text_color=color)
        self.after(1500, lambda: self.feedback_label.configure(text=""))


class RenderFleetApp(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("RenderFleet Commander")
        self.geometry("1000x600")

        self.config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
        self.syncthing_root = os.path.expanduser("~/RenderFleet")

        self.tabview = ctk.CTkTabview(self)
        self.tabview.pack(fill="both", expand=True, padx=10, pady=10)

        self.monitor_tab = self.tabview.add("Monitor")
        self.factory_tab = self.tabview.add("Image Factory")
        self.video_tab = self.tabview.add("Video Factory")
        self.weights_tab = self.tabview.add("Weights")
        self.analytics_tab = self.tabview.add("Analytics")

        header = ctk.CTkFrame(self.monitor_tab, fg_color="#111111")
        header.pack(fill="x", padx=10, pady=10)

        title = ctk.CTkLabel(header, text="Fleet Status", font=("Helvetica", 20, "bold"))
        title.pack(anchor="w", padx=12, pady=10)

        self.monitor_frame = ctk.CTkScrollableFrame(self.monitor_tab, fg_color="#151515")
        self.monitor_frame.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        self.worker_cards = {}
        self.empty_label = None

        self._build_image_factory()
        self._build_video_factory()
        self._build_weights_tab()
        self._build_analytics_tab()

        self.refresh_fleet()
        self.refresh_review_folders()
        self.refresh_analytics()

    def _build_image_factory(self):
        container = ctk.CTkFrame(self.factory_tab, fg_color="#151515")
        container.pack(fill="both", expand=True, padx=10, pady=10)

        header = ctk.CTkLabel(container, text="Image Factory", font=("Helvetica", 20, "bold"))
        header.pack(anchor="w", padx=12, pady=(12, 6))

        form = ctk.CTkFrame(container, fg_color="#1b1b1b", corner_radius=10)
        form.pack(fill="x", padx=12, pady=10)

        weights_row = ctk.CTkFrame(form, fg_color="transparent")
        weights_row.pack(fill="x", padx=12, pady=(12, 6))
        weight_label = ctk.CTkLabel(weights_row, text="Weight Key")
        weight_label.pack(side="left", padx=(0, 6))
        self.img_weight_menu = ctk.CTkOptionMenu(
            weights_row, values=self._get_weight_keys()
        )
        self.img_weight_menu.set("default")
        self.img_weight_menu.pack(side="left")
        self.img_vip_var = ctk.BooleanVar(value=False)
        self.img_vip_checkbox = ctk.CTkCheckBox(
            weights_row, text="VIP / Urgent", variable=self.img_vip_var
        )
        self.img_vip_checkbox.pack(side="left", padx=(12, 0))

        self.job_name_entry = ctk.CTkEntry(form, placeholder_text="JobName (e.g. project_alpha)")
        self.job_name_entry.pack(fill="x", padx=12, pady=(12, 6))

        self.job_id_entry = ctk.CTkEntry(form, placeholder_text="ID (optional suffix)")
        self.job_id_entry.pack(fill="x", padx=12, pady=6)

        self.input_mode = ctk.CTkSegmentedButton(
            form,
            values=["Direct Input", "Load File"],
            command=self._on_mode_change,
        )
        self.input_mode.pack(fill="x", padx=12, pady=6)
        self.input_mode.set("Direct Input")

        self.prompts_textbox = ctk.CTkTextbox(form, height=200)
        self.prompts_textbox.pack(fill="both", padx=12, pady=(6, 12))

        self.file_button = ctk.CTkButton(form, text="Select .txt File", command=self._select_file)
        self.file_button.pack_forget()

        self.loaded_file_content = ""

        self.submit_button = ctk.CTkButton(
            container,
            text="🚀 Submit Job",
            height=50,
            font=("Helvetica", 16, "bold"),
            command=self.submit_job,
        )
        self.submit_button.pack(fill="x", padx=12, pady=(6, 12))

        self.feedback_label = ctk.CTkLabel(container, text="", text_color="#2ecc71")
        self.feedback_label.pack(anchor="w", padx=12, pady=(0, 12))

    def _on_mode_change(self, value):
        if value == "Direct Input":
            self.file_button.pack_forget()
            self.prompts_textbox.pack(fill="both", padx=12, pady=(6, 12))
        else:
            self.prompts_textbox.pack_forget()
            self.file_button.pack(fill="x", padx=12, pady=(6, 12))

    def _select_file(self):
        file_path = ctk.filedialog.askopenfilename(
            title="Select prompt file",
            filetypes=[("Text Files", "*.txt")],
        )
        if not file_path:
            return
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                self.loaded_file_content = f.read()
        except OSError:
            self.loaded_file_content = ""

    def submit_job(self):
        job_name = self.job_name_entry.get().strip()
        job_id = self.job_id_entry.get().strip()
        mode = self.input_mode.get()

        if not job_name:
            self._show_feedback("Job Name is required.", is_error=True)
            return

        if mode == "Direct Input":
            prompts = self.prompts_textbox.get("1.0", "end").strip()
        else:
            prompts = self.loaded_file_content.strip()

        if not prompts:
            self._show_feedback("Prompts are required.", is_error=True)
            return

        weight_key = self.img_weight_menu.get() if hasattr(self, "img_weight_menu") else "default"
        base_name = f"{job_name}_{job_id}" if job_id else job_name
        if weight_key:
            base_name = f"{weight_key}_{base_name}"
        if self.img_vip_var.get():
            base_name = f"{base_name}_VIP"
        filename = f"{base_name}.txt"
        target_dir = os.path.join(self.syncthing_root, "01_job_factory", "img_queue")
        os.makedirs(target_dir, exist_ok=True)
        target_path = os.path.join(target_dir, filename)

        try:
            with open(target_path, "w", encoding="utf-8") as f:
                f.write(prompts)
        except OSError:
            self._show_feedback("Failed to write job file.", is_error=True)
            return

        self._show_feedback("✅ Job Sent!", is_error=False)
        self.job_name_entry.delete(0, "end")
        self.job_id_entry.delete(0, "end")
        self.prompts_textbox.delete("1.0", "end")
        self.loaded_file_content = ""

    def _show_feedback(self, message, is_error=False):
        color = "#d9534f" if is_error else "#2ecc71"
        self.feedback_label.configure(text=message, text_color=color)
        self.after(2000, lambda: self.feedback_label.configure(text=""))

    def _build_video_factory(self):
        container = ctk.CTkFrame(self.video_tab, fg_color="#151515")
        container.pack(fill="both", expand=True, padx=10, pady=10)

        header = ctk.CTkLabel(container, text="Video Factory", font=("Helvetica", 20, "bold"))
        header.pack(anchor="w", padx=12, pady=(12, 6))

        columns = ctk.CTkFrame(container, fg_color="transparent")
        columns.pack(fill="both", expand=True, padx=12, pady=10)

        left = ctk.CTkFrame(columns, fg_color="#1b1b1b", corner_radius=10)
        left.pack(side="left", fill="both", expand=True, padx=(0, 6), pady=0)

        right = ctk.CTkFrame(columns, fg_color="#1b1b1b", corner_radius=10)
        right.pack(side="left", fill="both", expand=True, padx=(6, 0), pady=0)

        left_label = ctk.CTkLabel(left, text="Ready for Production (in /_ready)", font=("Helvetica", 14, "bold"))
        left_label.pack(anchor="w", padx=12, pady=(12, 6))

        self.review_list_frame = ctk.CTkScrollableFrame(left, fg_color="#151515")
        self.review_list_frame.pack(fill="both", expand=True, padx=12, pady=6)

        self.refresh_list_button = ctk.CTkButton(left, text="Refresh List", command=self.refresh_review_folders)
        self.refresh_list_button.pack(fill="x", padx=12, pady=(6, 12))

        right_label = ctk.CTkLabel(right, text="Selected Job Config", font=("Helvetica", 14, "bold"))
        right_label.pack(anchor="w", padx=12, pady=(12, 6))

        weights_row = ctk.CTkFrame(right, fg_color="transparent")
        weights_row.pack(fill="x", padx=12, pady=(0, 6))
        weight_label = ctk.CTkLabel(weights_row, text="Weight Key")
        weight_label.pack(side="left", padx=(0, 6))
        self.vid_weight_menu = ctk.CTkOptionMenu(
            weights_row, values=self._get_weight_keys()
        )
        self.vid_weight_menu.set("default")
        self.vid_weight_menu.pack(side="left")
        self.vid_vip_var = ctk.BooleanVar(value=False)
        self.vid_vip_checkbox = ctk.CTkCheckBox(
            weights_row, text="VIP / Urgent", variable=self.vid_vip_var
        )
        self.vid_vip_checkbox.pack(side="left", padx=(12, 0))

        self.video_mode_tabs = ctk.CTkTabview(right)
        self.video_mode_tabs.pack(fill="both", expand=True, padx=12, pady=6)

        global_tab = self.video_mode_tabs.add("Global")
        manual_tab = self.video_mode_tabs.add("Manual")
        mapping_tab = self.video_mode_tabs.add("Mapping")

        self.global_prompt_entry = ctk.CTkEntry(global_tab, placeholder_text="e.g. slow pan, 4k")
        self.global_prompt_entry.pack(fill="x", padx=10, pady=10)

        self.manual_prompts_frame = ctk.CTkScrollableFrame(manual_tab, fg_color="#151515")
        self.manual_prompts_frame.pack(fill="both", expand=True, padx=10, pady=10)

        self.manual_prompt_entries = {}

        self.mapping_button = ctk.CTkButton(mapping_tab, text="Load Mapping File", command=self._load_mapping_file)
        self.mapping_button.pack(fill="x", padx=10, pady=(10, 6))
        self.mapping_status = ctk.CTkLabel(mapping_tab, text="No mapping loaded.")
        self.mapping_status.pack(anchor="w", padx=10, pady=(0, 6))
        self.mapping_input = ctk.CTkTextbox(mapping_tab, height=120)
        self.mapping_input.configure(state="normal")
        self.mapping_input.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        self.video_stats_label = ctk.CTkLabel(right, text="Stats: 0 Images found")
        self.video_stats_label.pack(anchor="w", padx=12, pady=6)

        self.dispatch_button = ctk.CTkButton(
            right,
            text="🎬 Dispatch to Video Queue",
            height=45,
            font=("Helvetica", 14, "bold"),
            command=self.dispatch_video_job,
        )
        self.dispatch_button.pack(fill="x", padx=12, pady=(6, 12))

        self.video_feedback_label = ctk.CTkLabel(right, text="", text_color="#2ecc71")
        self.video_feedback_label.pack(anchor="w", padx=12, pady=(0, 12))

        self.selected_review_folder = ""
        self.selected_images = []
        self.review_folders = []

    def refresh_review_folders(self):
        review_root = os.path.join(self.syncthing_root, "03_review_room", "_ready")
        try:
            os.makedirs(review_root, exist_ok=True)
        except OSError:
            pass
        try:
            entries = os.listdir(review_root)
        except OSError:
            entries = []

        folders = []
        for name in entries:
            full_path = os.path.join(review_root, name)
            if os.path.isdir(full_path):
                folders.append(name)

        self.review_folders = sorted(folders)
        self.selected_review_folder = ""
        self.selected_images = []
        self._update_review_list()
        self.video_stats_label.configure(text="Stats: 0 Images found")
        self._populate_manual_prompts()
        self._update_mapping_preview()

    def _update_review_list(self):
        for widget in self.review_list_frame.winfo_children():
            widget.destroy()

        if not self.review_folders:
            empty = ctk.CTkLabel(self.review_list_frame, text="No folders found.")
            empty.pack(anchor="w", padx=8, pady=8)
            return

        for name in self.review_folders:
            btn = ctk.CTkButton(
                self.review_list_frame,
                text=name,
                fg_color="#1f6feb",
                command=lambda n=name: self.select_folder(n),
            )
            btn.pack(fill="x", padx=6, pady=4)

    def select_folder(self, folder_name):
        self.selected_review_folder = os.path.join(
            self.syncthing_root, "03_review_room", "_ready", folder_name
        )
        weight_keys = self._get_weight_keys()
        for key in sorted(weight_keys, key=len, reverse=True):
            if folder_name.startswith(f"{key}_"):
                if hasattr(self, "vid_weight_menu"):
                    self.vid_weight_menu.set(key)
                break
        image_count = 0
        images = []
        if os.path.isdir(self.selected_review_folder):
            for fname in os.listdir(self.selected_review_folder):
                if fname.lower().endswith((".png", ".jpg", ".jpeg")):
                    image_count += 1
                    images.append(fname)
        self.selected_images = sorted(images)
        self.video_stats_label.configure(text=f"Stats: {image_count} Images found")
        self._populate_manual_prompts()
        self._update_mapping_preview()

    def _populate_manual_prompts(self):
        for widget in self.manual_prompts_frame.winfo_children():
            widget.destroy()
        self.manual_prompt_entries = {}
        if not self.selected_images:
            empty = ctk.CTkLabel(self.manual_prompts_frame, text="Select a folder to load images.")
            empty.pack(anchor="w", padx=8, pady=8)
            return

        for fname in self.selected_images:
            row = ctk.CTkFrame(self.manual_prompts_frame, fg_color="transparent")
            row.pack(fill="x", padx=6, pady=4)
            label = ctk.CTkLabel(row, text=fname, width=180, anchor="w")
            label.pack(side="left")
            entry = ctk.CTkEntry(row, placeholder_text="Prompt for this image")
            entry.pack(side="left", fill="x", expand=True, padx=(6, 0))
            self.manual_prompt_entries[fname] = entry

    def _load_mapping_file(self):
        file_path = ctk.filedialog.askopenfilename(
            title="Select mapping file",
            filetypes=[("Text Files", "*.txt")],
        )
        if not file_path:
            return
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                lines = f.read().splitlines()
        except OSError:
            lines = []

        try:
            self.mapping_input.delete("1.0", "end")
            self.mapping_input.insert("1.0", "\n".join(lines))
        except Exception:
            pass
        self.mapping_status.configure(text=f"Loaded {len(lines)} lines.")

    def _update_mapping_preview(self):
        return

    def _parse_mapping_from_text(self, text, images):
        image_numbers = []
        for name in images:
            match = re.search(r"\d+", name)
            if not match:
                continue
            image_numbers.append((name, int(match.group())))
        prompt_map = {}
        for raw_line in text.splitlines():
            line = raw_line.strip()
            if not line or "=" not in line:
                continue
            key, prompt = line.split("=", 1)
            key = key.strip()
            prompt = prompt.strip()
            if not key:
                continue
            key_match = re.search(r"\d+", key)
            if not key_match:
                continue
            key_number = int(key_match.group())
            for name, img_number in image_numbers:
                if img_number == key_number:
                    prompt_map[name] = prompt
        return prompt_map

    def dispatch_video_job(self):
        if not self.selected_review_folder:
            self._show_video_feedback("Select a folder first.", is_error=True)
            return

        image_files = self.selected_images[:]
        if not image_files:
            try:
                for fname in os.listdir(self.selected_review_folder):
                    if fname.lower().endswith((".png", ".jpg", ".jpeg")):
                        image_files.append(fname)
            except OSError:
                self._show_video_feedback("Failed to read folder.", is_error=True)
                return

        mode = self.video_mode_tabs.get()
        if mode == "Global":
            global_prompt = self.global_prompt_entry.get().strip()
            prompt_map = {fname: global_prompt for fname in image_files}
        elif mode == "Manual":
            prompt_map = {}
            for fname in image_files:
                entry = self.manual_prompt_entries.get(fname)
                prompt_map[fname] = entry.get().strip() if entry else ""
        else:
            mapping_text = ""
            if hasattr(self, "mapping_input"):
                mapping_text = self.mapping_input.get("1.0", "end")
            prompt_map = self._parse_mapping_from_text(mapping_text, image_files)

        rename_pairs = []
        for idx, old_name in enumerate(image_files, start=1):
            ext = os.path.splitext(old_name)[1].lower()
            new_name = f"image_{idx:03d}{ext}"
            rename_pairs.append((old_name, new_name))

        temp_pairs = []
        for old_name, new_name in rename_pairs:
            if old_name == new_name:
                continue
            temp_name = f"__rf_tmp_{new_name}"
            temp_pairs.append((old_name, temp_name, new_name))
            try:
                os.rename(
                    os.path.join(self.selected_review_folder, old_name),
                    os.path.join(self.selected_review_folder, temp_name),
                )
            except OSError:
                self._show_video_feedback("Failed to rename images.", is_error=True)
                return
            old_txt = os.path.splitext(old_name)[0] + ".txt"
            tmp_txt = os.path.splitext(temp_name)[0] + ".txt"
            if os.path.exists(os.path.join(self.selected_review_folder, old_txt)):
                try:
                    os.rename(
                        os.path.join(self.selected_review_folder, old_txt),
                        os.path.join(self.selected_review_folder, tmp_txt),
                    )
                except OSError:
                    self._show_video_feedback("Failed to rename prompt files.", is_error=True)
                    return

        for _old_name, temp_name, new_name in temp_pairs:
            try:
                os.rename(
                    os.path.join(self.selected_review_folder, temp_name),
                    os.path.join(self.selected_review_folder, new_name),
                )
            except OSError:
                self._show_video_feedback("Failed to finalize image renames.", is_error=True)
                return
            tmp_txt = os.path.splitext(temp_name)[0] + ".txt"
            new_txt = os.path.splitext(new_name)[0] + ".txt"
            if os.path.exists(os.path.join(self.selected_review_folder, tmp_txt)):
                try:
                    os.rename(
                        os.path.join(self.selected_review_folder, tmp_txt),
                        os.path.join(self.selected_review_folder, new_txt),
                    )
                except OSError:
                    self._show_video_feedback("Failed to finalize prompt renames.", is_error=True)
                    return

        prompt_map = {
            new_name: prompt_map.get(old_name, "")
            for old_name, new_name in rename_pairs
        }
        image_files = [new_name for _old_name, new_name in rename_pairs]

        for fname in image_files:
            base, _ext = os.path.splitext(fname)
            txt_path = os.path.join(self.selected_review_folder, f"{base}.txt")
            try:
                with open(txt_path, "w", encoding="utf-8") as f:
                    f.write(prompt_map.get(fname, ""))
            except OSError:
                self._show_video_feedback("Failed to write prompt files.", is_error=True)
                return

        weight_key = self.vid_weight_menu.get() if hasattr(self, "vid_weight_menu") else "default"
        folder_name = os.path.basename(self.selected_review_folder)
        if weight_key and not folder_name.startswith(f"{weight_key}_"):
            folder_name = f"{weight_key}_{folder_name}"
        if self.vid_vip_var.get() and "VIP" not in folder_name:
            folder_name = f"{folder_name}_VIP"
        target_dir = os.path.join(self.syncthing_root, "01_job_factory", "vid_queue")
        os.makedirs(target_dir, exist_ok=True)
        try:
            shutil.move(self.selected_review_folder, os.path.join(target_dir, folder_name))
        except OSError:
            self._show_video_feedback("Failed to move folder.", is_error=True)
            return

        self._show_video_feedback("Job sent to Video Queue!", is_error=False)
        self.refresh_review_folders()

    def _show_video_feedback(self, message, is_error=False):
        color = "#d9534f" if is_error else "#2ecc71"
        self.video_feedback_label.configure(text=message, text_color=color)
        self.after(2000, lambda: self.video_feedback_label.configure(text=""))

    def _build_weights_tab(self):
        container = ctk.CTkFrame(self.weights_tab, fg_color="#151515")
        container.pack(fill="both", expand=True, padx=10, pady=10)

        header = ctk.CTkLabel(container, text="Dispatch Weights", font=("Helvetica", 20, "bold"))
        header.pack(anchor="w", padx=12, pady=(12, 6))

        add_row = ctk.CTkFrame(container, fg_color="transparent")
        add_row.pack(fill="x", padx=12, pady=(0, 6))
        self.new_weight_name = ctk.CTkEntry(add_row, placeholder_text="weight name")
        self.new_weight_name.pack(side="left", padx=(0, 6))
        self.new_weight_value = ctk.CTkEntry(add_row, placeholder_text="value")
        self.new_weight_value.pack(side="left", padx=(0, 6))
        add_btn = ctk.CTkButton(add_row, text="+", width=36, command=self._add_weight_row)
        add_btn.pack(side="left")

        self.weights_frame = ctk.CTkFrame(container, fg_color="#1b1b1b", corner_radius=10)
        self.weights_frame.pack(fill="both", expand=True, padx=12, pady=10)

        self.weights_entries = {}
        self._load_weights_config()

        self.save_weights_button = ctk.CTkButton(
            container, text="Save Weights", height=40, command=self._save_weights
        )
        self.save_weights_button.pack(fill="x", padx=12, pady=(0, 6))

        self.weights_feedback = ctk.CTkLabel(container, text="", text_color="#2ecc71")
        self.weights_feedback.pack(anchor="w", padx=12, pady=(0, 12))

    def _load_weights_config(self):
        for widget in self.weights_frame.winfo_children():
            widget.destroy()
        self.weights_entries = {}
        settings_path = os.path.join(self.syncthing_root, "_system", "settings.json")
        try:
            with open(settings_path, "r", encoding="utf-8") as f:
                cfg = json.load(f)
        except (OSError, json.JSONDecodeError):
            cfg = {}

        weights = cfg.get("weights", {}) or {"default": 10}
        for key, value in weights.items():
            self._add_weight_row(name=key, value=str(value))

    def _add_weight_row(self, name=None, value=None):
        from_input = False
        if name is None:
            from_input = True
            name = self.new_weight_name.get().strip()
            value = self.new_weight_value.get().strip()
        if not name:
            return
        row = ctk.CTkFrame(self.weights_frame, fg_color="transparent")
        row.pack(fill="x", padx=12, pady=6)
        label = ctk.CTkLabel(row, text=name, width=120, anchor="w")
        label.pack(side="left")
        entry = ctk.CTkEntry(row)
        entry.insert(0, value if value is not None else "")
        entry.pack(side="left", fill="x", expand=True, padx=(0, 6))
        del_btn = ctk.CTkButton(row, text="X", width=36, command=lambda n=name: self._delete_weight_row(n))
        del_btn.pack(side="left")
        self.weights_entries[name] = (entry, row)
        if from_input:
            self.new_weight_name.delete(0, "end")
            self.new_weight_value.delete(0, "end")

    def _delete_weight_row(self, name):
        entry, row = self.weights_entries.get(name, (None, None))
        if row is not None:
            row.destroy()
        if name in self.weights_entries:
            del self.weights_entries[name]

    def _save_weights(self):
        weights = {}
        for key, entry_row in self.weights_entries.items():
            entry = entry_row[0]
            try:
                weights[key] = int(entry.get().strip())
            except ValueError:
                continue

        settings_path = os.path.join(self.syncthing_root, "_system", "settings.json")
        try:
            with open(settings_path, "r", encoding="utf-8") as f:
                cfg = json.load(f)
        except (OSError, json.JSONDecodeError):
            cfg = {}
        cfg["weights"] = weights
        try:
            os.makedirs(os.path.dirname(settings_path), exist_ok=True)
            with open(settings_path, "w", encoding="utf-8") as f:
                json.dump(cfg, f, indent=4)
            self._show_weights_feedback("Weights saved.")
        except OSError:
            self._show_weights_feedback("Failed to save weights.", is_error=True)

    def _show_weights_feedback(self, message, is_error=False):
        color = "#d9534f" if is_error else "#2ecc71"
        self.weights_feedback.configure(text=message, text_color=color)
        self.after(2000, lambda: self.weights_feedback.configure(text=""))

    def _build_analytics_tab(self):
        container = ctk.CTkFrame(self.analytics_tab, fg_color="#151515")
        container.pack(fill="both", expand=True, padx=10, pady=10)

        header = ctk.CTkLabel(container, text="Analytics", font=("Helvetica", 20, "bold"))
        header.pack(anchor="w", padx=12, pady=(12, 6))

        stats = ctk.CTkFrame(container, fg_color="#1b1b1b", corner_radius=10)
        stats.pack(fill="both", expand=True, padx=12, pady=10)

        self.queue_label = ctk.CTkLabel(stats, text="Jobs in Queue (Img/Vid): 0 / 0")
        self.queue_label.pack(anchor="w", padx=12, pady=(12, 6))

        self.active_workers_label = ctk.CTkLabel(stats, text="Active Workers: 0")
        self.active_workers_label.pack(anchor="w", padx=12, pady=6)

        self.eta_label = ctk.CTkLabel(stats, text="Est. Throughput: N/A")
        self.eta_label.pack(anchor="w", padx=12, pady=(6, 12))

        self.throughput_24h_label = ctk.CTkLabel(stats, text="Est. 24h Throughput: N/A")
        self.throughput_24h_label.pack(anchor="w", padx=12, pady=(0, 12))

    def refresh_fleet(self):
        heartbeat_dir = os.path.join(self.syncthing_root, "_system", "heartbeats")
        heartbeat_files = glob.glob(os.path.join(heartbeat_dir, "*.json"))
        current_time = time.time()
        seen_ids = set()

        for hb_path in heartbeat_files:
            try:
                with open(hb_path, "r", encoding="utf-8") as f:
                    data = json.load(f)

                worker_id = data.get("worker_id")
                if not worker_id:
                    continue

                heartbeat_timestamp = data.get("timestamp", 0)
                time_diff = current_time - heartbeat_timestamp

                last_seen = f"{int(time_diff)}s ago"
                data["time_diff"] = time_diff
                data["last_seen"] = last_seen

                is_online = time_diff <= 90

                if worker_id in self.worker_cards:
                    self.worker_cards[worker_id].update_state(data, is_online)
                else:
                    card = WorkerCard(self.monitor_frame, data, is_online)
                    card.pack(fill="x", padx=12, pady=8)
                    self.worker_cards[worker_id] = card

                seen_ids.add(worker_id)
            except (OSError, json.JSONDecodeError):
                continue

        for worker_id in list(self.worker_cards.keys()):
            if worker_id not in seen_ids:
                self.worker_cards[worker_id].destroy()
                del self.worker_cards[worker_id]

        if not self.worker_cards:
            if not self.empty_label:
                self.empty_label = ctk.CTkLabel(self.monitor_frame, text="No workers found.")
                self.empty_label.pack(padx=12, pady=12, anchor="w")
        else:
            if self.empty_label:
                self.empty_label.destroy()
                self.empty_label = None

        self.after(2000, self.refresh_fleet)

    def refresh_analytics(self):
        img_queue = os.path.join(self.syncthing_root, "01_job_factory", "img_queue")
        vid_queue = os.path.join(self.syncthing_root, "01_job_factory", "vid_queue")
        try:
            img_jobs = [f for f in os.listdir(img_queue) if not f.startswith(".")]
        except OSError:
            img_jobs = []
        try:
            vid_jobs = [f for f in os.listdir(vid_queue) if not f.startswith(".")]
        except OSError:
            vid_jobs = []

        heartbeat_dir = os.path.join(self.syncthing_root, "_system", "heartbeats")
        heartbeat_files = glob.glob(os.path.join(heartbeat_dir, "*.json"))
        current_time = time.time()
        active_workers = 0
        for hb_path in heartbeat_files:
            try:
                with open(hb_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                ts = data.get("timestamp", 0)
                if current_time - ts <= 90:
                    active_workers += 1
            except (OSError, json.JSONDecodeError):
                continue

        self.queue_label.configure(text=f"Jobs in Queue (Img/Vid): {len(img_jobs)} / {len(vid_jobs)}")
        self.active_workers_label.configure(text=f"Active Workers: {active_workers}")

        total_jobs = len(img_jobs) + len(vid_jobs)
        if active_workers <= 0:
            eta_text = "Est. Throughput: N/A"
        else:
            eta_seconds = int((total_jobs * 30) / max(active_workers, 1))
            eta_text = f"Est. Throughput: ~{eta_seconds}s ETA"
        self.eta_label.configure(text=eta_text)

        if active_workers <= 0:
            throughput_text = "Est. 24h Throughput: N/A"
        else:
            throughput = int((active_workers * 86400) / 30)
            throughput_text = f"Est. 24h Throughput: {throughput} Videos"
        self.throughput_24h_label.configure(text=throughput_text)

        self.after(5000, self.refresh_analytics)

    def _get_weight_keys(self):
        settings_path = os.path.join(self.syncthing_root, "_system", "settings.json")
        try:
            with open(settings_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            weights = data.get("weights", {})
            keys = [k for k in weights.keys() if isinstance(k, str)]
        except (OSError, json.JSONDecodeError, AttributeError):
            keys = []
        if "default" not in keys:
            keys.insert(0, "default")
        return keys or ["default"]


if __name__ == "__main__":
    app = RenderFleetApp()
    app.mainloop()
