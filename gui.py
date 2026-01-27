import customtkinter as ctk
import os
import json
import time
import glob
from datetime import datetime
import shutil

ctk.set_appearance_mode("Dark")
ctk.set_default_color_theme("dark-blue")


class WorkerCard(ctk.CTkFrame):
    def __init__(self, master, worker_data, is_online):
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

        super().__init__(master, fg_color="#1f1f1f", border_width=2, border_color=border_color, corner_radius=10)

        worker_id = worker_data.get("worker_id", "Unknown")
        role = worker_data.get("role", "Unknown")
        current_job = worker_data.get("current_job", "None")
        last_seen = worker_data.get("last_seen", "Unknown")

        title = ctk.CTkLabel(self, text=f"Worker ID: {worker_id}", font=("Helvetica", 16, "bold"))
        title.pack(anchor="w", padx=12, pady=(10, 2))

        status_label = ctk.CTkLabel(self, text=f"Status: {status_text}")
        status_label.pack(anchor="w", padx=12)

        role_label = ctk.CTkLabel(self, text=f"Role: {role}")
        role_label.pack(anchor="w", padx=12)

        job_label = ctk.CTkLabel(self, text=f"Current Job: {current_job}")
        job_label.pack(anchor="w", padx=12)

        last_seen_label = ctk.CTkLabel(self, text=f"Last Seen: {last_seen}")
        last_seen_label.pack(anchor="w", padx=12, pady=(0, 6))

        btn_state = "normal" if is_online and time_diff <= 90 else "disabled"

        power_row = ctk.CTkFrame(self, fg_color="transparent")
        power_row.pack(fill="x", padx=12, pady=(0, 6))

        start_btn = ctk.CTkButton(
            power_row,
            text="▶️ START",
            width=110,
            height=26,
            fg_color="green",
            command=lambda: self.send_command(worker_id, "start"),
            state=btn_state,
        )
        start_btn.grid(row=0, column=0, padx=4, pady=4, sticky="w")

        stop_btn = ctk.CTkButton(
            power_row,
            text="⏸ STOP",
            width=110,
            height=26,
            fg_color="red",
            command=lambda: self.send_command(worker_id, "stop"),
            state=btn_state,
        )
        stop_btn.grid(row=0, column=1, padx=4, pady=4, sticky="w")

        button_row = ctk.CTkFrame(self, fg_color="transparent")
        button_row.pack(fill="x", padx=12, pady=(0, 8))

        img_btn = ctk.CTkButton(
            button_row,
            text="Set: ImgLead",
            width=110,
            height=26,
            command=lambda: self.send_command(worker_id, "set_role", "img_lead"),
            state=btn_state,
        )
        img_btn.grid(row=0, column=0, padx=4, pady=4, sticky="w")

        vid_btn = ctk.CTkButton(
            button_row,
            text="Set: VidLead",
            width=110,
            height=26,
            command=lambda: self.send_command(worker_id, "set_role", "vid_lead"),
            state=btn_state,
        )
        vid_btn.grid(row=0, column=1, padx=4, pady=4, sticky="w")

        img_worker_btn = ctk.CTkButton(
            button_row,
            text="Set: ImgWork",
            width=110,
            height=26,
            command=lambda: self.send_command(worker_id, "set_role", "img_worker"),
            state=btn_state,
        )
        img_worker_btn.grid(row=1, column=0, padx=4, pady=4, sticky="w")

        vid_worker_btn = ctk.CTkButton(
            button_row,
            text="Set: VidWork",
            width=110,
            height=26,
            command=lambda: self.send_command(worker_id, "set_role", "vid_worker"),
            state=btn_state,
        )
        vid_worker_btn.grid(row=1, column=1, padx=4, pady=4, sticky="w")

        self.feedback_label = ctk.CTkLabel(self, text="", text_color="#2ecc71")
        self.feedback_label.pack(anchor="w", padx=12, pady=(0, 10))

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

        self.syncthing_root = os.path.expanduser("~/RenderFleet")

        self.tabview = ctk.CTkTabview(self)
        self.tabview.pack(fill="both", expand=True, padx=10, pady=10)

        self.monitor_tab = self.tabview.add("Monitor")
        self.factory_tab = self.tabview.add("Image Factory")
        self.video_tab = self.tabview.add("Video Factory")

        header = ctk.CTkFrame(self.monitor_tab, fg_color="#111111")
        header.pack(fill="x", padx=10, pady=10)

        title = ctk.CTkLabel(header, text="Fleet Status", font=("Helvetica", 20, "bold"))
        title.pack(anchor="w", padx=12, pady=10)

        self.monitor_frame = ctk.CTkScrollableFrame(self.monitor_tab, fg_color="#151515")
        self.monitor_frame.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        self._build_image_factory()
        self._build_video_factory()

        self.refresh_fleet()
        self.refresh_review_folders()

    def _build_image_factory(self):
        container = ctk.CTkFrame(self.factory_tab, fg_color="#151515")
        container.pack(fill="both", expand=True, padx=10, pady=10)

        header = ctk.CTkLabel(container, text="Image Factory", font=("Helvetica", 20, "bold"))
        header.pack(anchor="w", padx=12, pady=(12, 6))

        form = ctk.CTkFrame(container, fg_color="#1b1b1b", corner_radius=10)
        form.pack(fill="x", padx=12, pady=10)

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

        filename = f"{job_name}_{job_id}.txt" if job_id else f"{job_name}.txt"
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

        self.video_prompt_entry = ctk.CTkEntry(right, placeholder_text="e.g. slow pan, 4k")
        self.video_prompt_entry.pack(fill="x", padx=12, pady=6)

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
        self._update_review_list()
        self.video_stats_label.configure(text="Stats: 0 Images found")

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
        image_count = 0
        if os.path.isdir(self.selected_review_folder):
            for fname in os.listdir(self.selected_review_folder):
                if fname.lower().endswith((".png", ".jpg", ".jpeg")):
                    image_count += 1
        self.video_stats_label.configure(text=f"Stats: {image_count} Images found")

    def dispatch_video_job(self):
        if not self.selected_review_folder:
            self._show_video_feedback("Select a folder first.", is_error=True)
            return

        prompt = self.video_prompt_entry.get().strip()
        image_files = []
        try:
            for fname in os.listdir(self.selected_review_folder):
                if fname.lower().endswith((".png", ".jpg", ".jpeg")):
                    image_files.append(fname)
        except OSError:
            self._show_video_feedback("Failed to read folder.", is_error=True)
            return

        for fname in image_files:
            base, _ext = os.path.splitext(fname)
            txt_path = os.path.join(self.selected_review_folder, f"{base}.txt")
            try:
                with open(txt_path, "w", encoding="utf-8") as f:
                    f.write(prompt)
            except OSError:
                self._show_video_feedback("Failed to write prompt files.", is_error=True)
                return

        target_dir = os.path.join(self.syncthing_root, "01_job_factory", "vid_queue")
        os.makedirs(target_dir, exist_ok=True)
        try:
            shutil.move(self.selected_review_folder, target_dir)
        except OSError:
            self._show_video_feedback("Failed to move folder.", is_error=True)
            return

        self._show_video_feedback("Job sent to Video Queue!", is_error=False)
        self.refresh_review_folders()

    def _show_video_feedback(self, message, is_error=False):
        color = "#d9534f" if is_error else "#2ecc71"
        self.video_feedback_label.configure(text=message, text_color=color)
        self.after(2000, lambda: self.video_feedback_label.configure(text=""))

    def refresh_fleet(self):
        heartbeat_dir = os.path.join(self.syncthing_root, "_system", "heartbeats")
        heartbeat_files = glob.glob(os.path.join(heartbeat_dir, "*.json"))
        current_time = time.time()

        for widget in self.monitor_frame.winfo_children():
            widget.destroy()

        if not heartbeat_files:
            empty = ctk.CTkLabel(self.monitor_frame, text="No workers found.")
            empty.pack(padx=12, pady=12, anchor="w")
        else:
            for hb_path in heartbeat_files:
                try:
                    with open(hb_path, "r", encoding="utf-8") as f:
                        data = json.load(f)

                    heartbeat_timestamp = data.get("timestamp", 0)
                    time_diff = current_time - heartbeat_timestamp

                    last_seen = f"{int(time_diff)}s ago"
                    data["time_diff"] = time_diff
                    data["last_seen"] = last_seen

                    is_online = time_diff <= 90

                    card = WorkerCard(self.monitor_frame, data, is_online)
                    card.pack(fill="x", padx=12, pady=8)
                except (OSError, json.JSONDecodeError):
                    continue

        self.after(2000, self.refresh_fleet)


if __name__ == "__main__":
    app = RenderFleetApp()
    app.mainloop()
