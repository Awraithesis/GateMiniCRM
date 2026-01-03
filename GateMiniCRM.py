#!/usr/bin/env python3
"""
GateMiniCRM — tiny, offline CRM/ERP for a consulting business.
- Cross-platform: Windows & macOS (Python 3.9+)
- Single-file UI (Tkinter) + SQLite storage in user home
- Tabs: Companies, Contacts, Deals, Activities (notes/tasks), Time (billable), Invoices
- CSV import/export, Passcode lock (optional), Invoice PDF export (ReportLab)
- Offline two-way sync via JSONL packs (gate_sync.py module)

Packaging (Windows .exe):
    pip install pyinstaller reportlab
    pyinstaller --onefile --windowed --name GateMiniCRM GateMiniCRM.py
"""
from __future__ import annotations

import os
import sqlite3
import csv
import hashlib
import secrets
from datetime import datetime, date
from typing import Optional

import tkinter as tk
from tkinter import ttk, messagebox, filedialog, simpledialog

APP_NAME = "GateMiniCRM"
DB_PATH = os.path.join(os.path.expanduser("~"), f"{APP_NAME}.sqlite3")
DATE_FMT = "%Y-%m-%d"

PIPELINE_STAGES = ["Lead", "Qualified", "Proposal", "Won", "Lost"]
ACTIVITY_KINDS = ["Call", "Email", "Meeting", "Note", "Task"]

# Optional ReportLab for PDF invoices
try:
    from reportlab.lib.pagesizes import LETTER
    from reportlab.pdfgen import canvas as pdf_canvas
    HAVE_REPORTLAB = True
except Exception:
    HAVE_REPORTLAB = False

# Sync/merge helpers (external module)
try:
    from gate_sync import (
        ensure_change_tracking,
        export_changes_dialog,
        import_pack_dialog,
        init_device_settings_dialog,
    )
except Exception:
    ensure_change_tracking = export_changes_dialog = import_pack_dialog = init_device_settings_dialog = None

# -----------------
# Database helpers
# -----------------

def connect_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = connect_db()
    cur = conn.cursor()
    cur.executescript(
        """
        PRAGMA journal_mode=WAL;

        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        );

        CREATE TABLE IF NOT EXISTS companies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            phone TEXT,
            email TEXT,
            website TEXT,
            address TEXT,
            created_at TEXT NOT NULL DEFAULT (DATE('now'))
        );

        CREATE TABLE IF NOT EXISTS contacts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER,
            name TEXT NOT NULL,
            title TEXT,
            email TEXT,
            phone TEXT,
            FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE SET NULL
        );

        CREATE TABLE IF NOT EXISTS deals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER NOT NULL,
            contact_id INTEGER,
            title TEXT NOT NULL,
            value REAL NOT NULL DEFAULT 0,
            stage TEXT NOT NULL DEFAULT 'Lead',
            created_at TEXT NOT NULL DEFAULT (DATE('now')),
            close_date TEXT,
            notes TEXT,
            FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE CASCADE,
            FOREIGN KEY(contact_id) REFERENCES contacts(id) ON DELETE SET NULL
        );

        CREATE TABLE IF NOT EXISTS activities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            deal_id INTEGER,
            company_id INTEGER,
            kind TEXT NOT NULL,
            note TEXT,
            due_date TEXT,
            done INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (DATETIME('now')),
            FOREIGN KEY(deal_id) REFERENCES deals(id) ON DELETE CASCADE,
            FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE CASCADE
        );

        -- Billable time entries
        CREATE TABLE IF NOT EXISTS time_entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            deal_id INTEGER NOT NULL,
            work_date TEXT NOT NULL,
            hours REAL NOT NULL,
            rate REAL NOT NULL,
            notes TEXT,
            FOREIGN KEY(deal_id) REFERENCES deals(id) ON DELETE CASCADE
        );

        -- Invoices
        CREATE TABLE IF NOT EXISTS invoices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            deal_id INTEGER NOT NULL,
            issue_date TEXT NOT NULL,
            due_date TEXT,
            status TEXT NOT NULL DEFAULT 'Draft',
            notes TEXT,
            FOREIGN KEY(deal_id) REFERENCES deals(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS invoice_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            invoice_id INTEGER NOT NULL,
            description TEXT NOT NULL,
            qty REAL NOT NULL DEFAULT 1,
            unit_price REAL NOT NULL DEFAULT 0,
            FOREIGN KEY(invoice_id) REFERENCES invoices(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_contacts_company ON contacts(company_id);
        CREATE INDEX IF NOT EXISTS idx_deals_company ON deals(company_id);
        CREATE INDEX IF NOT EXISTS idx_activities_due ON activities(due_date, done);
        CREATE INDEX IF NOT EXISTS idx_time_deal ON time_entries(deal_id);
        CREATE INDEX IF NOT EXISTS idx_items_invoice ON invoice_items(invoice_id);
        """
    )
    conn.commit()
    conn.close()


# ------------
# Passcode UI
# ------------

def get_setting(conn: sqlite3.Connection, key: str) -> Optional[str]:
    cur = conn.cursor()
    cur.execute("SELECT value FROM settings WHERE key=?", (key,))
    r = cur.fetchone()
    return r[0] if r else None


def set_setting(conn: sqlite3.Connection, key: str, value: str) -> None:
    cur = conn.cursor()
    cur.execute("REPLACE INTO settings(key,value) VALUES(?,?)", (key, value))
    conn.commit()


def hash_pass(passcode: str, salt: str) -> str:
    return hashlib.sha256((salt + passcode).encode("utf-8")).hexdigest()


def ensure_passcode(conn: sqlite3.Connection, root: tk.Tk) -> bool:
    stored = get_setting(conn, "pass_hash")
    if not stored:
        if messagebox.askyesno(APP_NAME, "Set a passcode to open the app? (Recommended)"):
            while True:
                p1 = simpledialog.askstring(APP_NAME, "Create passcode", show='*', parent=root)
                if p1 is None:
                    break
                p2 = simpledialog.askstring(APP_NAME, "Confirm passcode", show='*', parent=root)
                if p2 != p1:
                    messagebox.showerror(APP_NAME, "Passcodes do not match. Try again.")
                    continue
                salt = secrets.token_hex(8)
                set_setting(conn, "pass_salt", salt)
                set_setting(conn, "pass_hash", hash_pass(p1, salt))
                messagebox.showinfo(APP_NAME, "Passcode set.")
                break
        return True
    salt = get_setting(conn, "pass_salt") or ""
    for _ in range(3):
        entered = simpledialog.askstring(APP_NAME, "Enter passcode", show='*', parent=root)
        if entered is None:
            return False
        if hash_pass(entered, salt) == stored:
            return True
        messagebox.showerror(APP_NAME, "Incorrect passcode.")
    return False


# ----------------
# CSV helpers
# ----------------

def ask_export_csv(headers, rows):
    path = filedialog.asksaveasfilename(
        defaultextension=".csv", filetypes=[("CSV", "*.csv")], title="Export to CSV"
    )
    if not path:
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for r in rows:
            writer.writerow([r.get(h, "") for h in headers])
    messagebox.showinfo(APP_NAME, f"Exported {len(rows)} rows to\n{path}")


def ask_import_csv(kind: str, conn: sqlite3.Connection):
    path = filedialog.askopenfilename(filetypes=[("CSV", "*.csv")], title=f"Import {kind} CSV")
    if not path:
        return
    cur = conn.cursor()
    added = 0
    with open(path, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        if kind == "companies":
            for r in reader:
                name = (r.get('name') or r.get('Name') or '').strip()
                if not name:
                    continue
                cur.execute(
                    "INSERT INTO companies(name, phone, email, website, address) VALUES(?,?,?,?,?)",
                    (name, r.get('phone') or '', r.get('email') or '', r.get('website') or '', r.get('address') or '')
                )
                added += 1
        elif kind == "contacts":
            for r in reader:
                name = (r.get('name') or r.get('Name') or '').strip()
                if not name:
                    continue
                company = (r.get('company') or r.get('Company') or '').strip()
                company_id = None
                if company:
                    cur.execute("SELECT id FROM companies WHERE name=?", (company,))
                    got = cur.fetchone()
                    if got:
                        company_id = got['id']
                    else:
                        cur.execute("INSERT INTO companies(name) VALUES(?)", (company,))
                        company_id = cur.lastrowid
                cur.execute(
                    "INSERT INTO contacts(company_id, name, title, email, phone) VALUES(?,?,?,?,?)",
                    (company_id, name, r.get('title') or '', r.get('email') or '', r.get('phone') or '')
                )
                added += 1
        conn.commit()
    messagebox.showinfo(APP_NAME, f"Imported {added} {kind} from\n{path}")


# --------------
# UI base class
# --------------

class ListFormBase(ttk.Frame):
    columns = []  # list[tuple[str,int]]

    def __init__(self, master, conn: sqlite3.Connection):
        super().__init__(master)
        self.conn = conn
        self.build()

    def build(self):
        container = ttk.PanedWindow(self, orient=tk.HORIZONTAL)
        container.pack(fill=tk.BOTH, expand=True)

        left = ttk.Frame(container)

        # Treeview expects a list of column IDs (strings), not tuples
        col_ids = [c for c, _ in self.columns]
        self.tree = ttk.Treeview(left, columns=col_ids, show="headings", selectmode="browse")

        vsb = ttk.Scrollbar(left, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        vsb.pack(side=tk.RIGHT, fill=tk.Y)

        # Configure headings/widths
        for col, w in self.columns:
            self.tree.heading(col, text=col)
            self.tree.column(col, width=w, anchor=tk.W)

        self.tree.bind("<<TreeviewSelect>>", lambda e: self.on_select())

        right = ttk.Frame(container, padding=8)
        self.form = ttk.Frame(right)
        self.form.pack(fill=tk.X)
        btns = ttk.Frame(right)
        btns.pack(fill=tk.X, pady=(8, 0))
        ttk.Button(btns, text="New", command=self.on_new).pack(side=tk.LEFT)
        ttk.Button(btns, text="Save", command=self.on_save).pack(side=tk.LEFT, padx=6)
        ttk.Button(btns, text="Delete", command=self.on_delete).pack(side=tk.LEFT)
        ttk.Button(btns, text="Clear", command=self.clear_form).pack(side=tk.LEFT, padx=6)

        container.add(left, weight=3)
        container.add(right, weight=2)

    def refresh(self):  # override
        pass

    def on_select(self):  # override
        pass

    def on_new(self):  # override
        pass

    def on_save(self):  # override
        pass

    def on_delete(self):  # override
        pass

    def clear_tree(self):
        for iid in self.tree.get_children():
            self.tree.delete(iid)

    def clear_form(self):
        for child in self.form.winfo_children():
            if isinstance(child, ttk.Entry):
                child.delete(0, tk.END)
            elif isinstance(child, ttk.Combobox):
                child.set("")
            elif isinstance(child, tk.Text):
                child.delete("1.0", tk.END)


# -------------
# Companies
# -------------
class CompaniesTab(ListFormBase):
    columns = [("ID", 50), ("Name", 220), ("Phone", 120), ("Email", 200)]

    def build(self):
        super().build()
        self.var_id = tk.StringVar()
        self.var_name = tk.StringVar()
        self.var_phone = tk.StringVar()
        self.var_email = tk.StringVar()
        self.var_website = tk.StringVar()
        self.txt_address = tk.Text(self.form, height=4, width=40)

        row = 0
        for label, widget in [
            ("ID", ttk.Entry(self.form, textvariable=self.var_id, state="readonly")),
            ("Name", ttk.Entry(self.form, textvariable=self.var_name)),
            ("Phone", ttk.Entry(self.form, textvariable=self.var_phone)),
            ("Email", ttk.Entry(self.form, textvariable=self.var_email)),
            ("Website", ttk.Entry(self.form, textvariable=self.var_website)),
            ("Address", self.txt_address),
        ]:
            ttk.Label(self.form, text=label).grid(row=row, column=0, sticky=tk.W, pady=2)
            widget.grid(row=row, column=1, sticky=tk.EW, pady=2)
            row += 1
        self.form.grid_columnconfigure(1, weight=1)

    def refresh(self):
        cur = self.conn.cursor()
        cur.execute("SELECT id, name, phone, email FROM companies ORDER BY name")
        self.clear_tree()
        for r in cur.fetchall():
            self.tree.insert("", tk.END, iid=r["id"], values=(r["id"], r["name"], r["phone"], r["email"]))

    def on_select(self):
        sel = self.tree.focus()
        if not sel:
            return
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM companies WHERE id=?", (sel,))
        r = cur.fetchone()
        if r:
            self.var_id.set(r["id"])
            self.var_name.set(r["name"])
            self.var_phone.set(r["phone"] or "")
            self.var_email.set(r["email"] or "")
            self.var_website.set(r["website"] or "")
            self.txt_address.delete("1.0", tk.END)
            self.txt_address.insert(tk.END, r["address"] or "")

    def on_new(self):
        self.clear_form()
        self.var_id.set("")

    def on_save(self):
        name = self.var_name.get().strip()
        if not name:
            messagebox.showwarning(APP_NAME, "Company name is required")
            return
        phone = self.var_phone.get().strip()
        email = self.var_email.get().strip()
        website = self.var_website.get().strip()
        address = self.txt_address.get("1.0", tk.END).strip()
        cur = self.conn.cursor()
        if self.var_id.get():
            cur.execute(
                "UPDATE companies SET name=?, phone=?, email=?, website=?, address=? WHERE id=?",
                (name, phone, email, website, address, self.var_id.get()),
            )
        else:
            cur.execute(
                "INSERT INTO companies(name, phone, email, website, address) VALUES(?,?,?,?,?)",
                (name, phone, email, website, address),
            )
            self.var_id.set(cur.lastrowid)
        self.conn.commit()
        self.refresh()

    def on_delete(self):
        if not self.var_id.get():
            return
        if messagebox.askyesno(APP_NAME, "Delete this company and related deals/activities?"):
            cur = self.conn.cursor()
            cur.execute("DELETE FROM companies WHERE id=?", (self.var_id.get(),))
            self.conn.commit()
            self.on_new()
            self.refresh()


# -------------
# Contacts
# -------------
class ContactsTab(ListFormBase):
    columns = [("ID", 50), ("Name", 200), ("Company", 200), ("Email", 200), ("Phone", 120)]

    def build(self):
        super().build()
        self.var_id = tk.StringVar()
        self.var_name = tk.StringVar()
        self.var_title = tk.StringVar()
        self.var_email = tk.StringVar()
        self.var_phone = tk.StringVar()
        self.company_cb = ttk.Combobox(self.form, values=self.company_options())

        row = 0
        for label, widget in [
            ("ID", ttk.Entry(self.form, textvariable=self.var_id, state="readonly")),
            ("Name", ttk.Entry(self.form, textvariable=self.var_name)),
            ("Title", ttk.Entry(self.form, textvariable=self.var_title)),
            ("Email", ttk.Entry(self.form, textvariable=self.var_email)),
            ("Phone", ttk.Entry(self.form, textvariable=self.var_phone)),
            ("Company", self.company_cb),
        ]:
            ttk.Label(self.form, text=label).grid(row=row, column=0, sticky=tk.W, pady=2)
            widget.grid(row=row, column=1, sticky=tk.EW, pady=2)
            row += 1
        self.form.grid_columnconfigure(1, weight=1)

    def company_options(self):
        cur = self.conn.cursor()
        cur.execute("SELECT id, name FROM companies ORDER BY name")
        return [f"{r['id']} - {r['name']}" for r in cur.fetchall()]

    def refresh(self):
        self.company_cb["values"] = self.company_options()
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT c.id, c.name, co.name as company, c.email, c.phone
            FROM contacts c LEFT JOIN companies co ON c.company_id = co.id
            ORDER BY c.name
            """
        )
        self.clear_tree()
        for r in cur.fetchall():
            self.tree.insert("", tk.END, iid=r["id"], values=(r["id"], r["name"], r["company"] or "", r["email"] or "", r["phone"] or ""))

    def on_select(self):
        sel = self.tree.focus()
        if not sel:
            return
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM contacts WHERE id=?", (sel,))
        r = cur.fetchone()
        if r:
            self.var_id.set(r["id"])
            self.var_name.set(r["name"])
            self.var_title.set(r["title"] or "")
            self.var_email.set(r["email"] or "")
            self.var_phone.set(r["phone"] or "")
            if r["company_id"]:
                cur.execute("SELECT name FROM companies WHERE id=?", (r["company_id"],))
                cname = cur.fetchone()["name"]
                self.company_cb.set(f"{r['company_id']} - {cname}")
            else:
                self.company_cb.set("")

    def on_new(self):
        self.clear_form()
        self.var_id.set("")

    def on_save(self):
        name = self.var_name.get().strip()
        if not name:
            messagebox.showwarning(APP_NAME, "Contact name is required")
            return
        title = self.var_title.get().strip()
        email = self.var_email.get().strip()
        phone = self.var_phone.get().strip()
        company_id = None
        if self.company_cb.get():
            try:
                company_id = int(self.company_cb.get().split(" - ")[0])
            except Exception:
                company_id = None
        cur = self.conn.cursor()
        if self.var_id.get():
            cur.execute(
                "UPDATE contacts SET name=?, title=?, email=?, phone=?, company_id=? WHERE id=?",
                (name, title, email, phone, company_id, self.var_id.get()),
            )
        else:
            cur.execute(
                "INSERT INTO contacts(company_id, name, title, email, phone) VALUES(?,?,?,?,?)",
                (company_id, name, title, email, phone),
            )
            self.var_id.set(cur.lastrowid)
        self.conn.commit()
        self.refresh()

    def on_delete(self):
        if not self.var_id.get():
            return
        if messagebox.askyesno(APP_NAME, "Delete this contact?"):
            cur = self.conn.cursor()
            cur.execute("DELETE FROM contacts WHERE id=?", (self.var_id.get(),))
            self.conn.commit()
            self.on_new()
            self.refresh()


# -------------
# Deals
# -------------
class DealsTab(ListFormBase):
    columns = [("ID", 50), ("Title", 220), ("Company", 180), ("Contact", 160), ("Stage", 100), ("Value", 90), ("Created", 100), ("Close", 100)]

    def build(self):
        super().build()
        self.var_id = tk.StringVar()
        self.var_title = tk.StringVar()
        self.var_value = tk.StringVar()
        self.stage_cb = ttk.Combobox(self.form, values=PIPELINE_STAGES)
        self.var_created = tk.StringVar(value=date.today().strftime(DATE_FMT))
        self.var_close = tk.StringVar()
        self.txt_notes = tk.Text(self.form, height=5, width=40)
        self.company_cb = ttk.Combobox(self.form, values=self.company_options())
        self.contact_cb = ttk.Combobox(self.form, values=self.contact_options(None))

        row = 0
        for label, widget in [
            ("ID", ttk.Entry(self.form, textvariable=self.var_id, state="readonly")),
            ("Title", ttk.Entry(self.form, textvariable=self.var_title)),
            ("Company", self.company_cb),
            ("Contact", self.contact_cb),
            ("Stage", self.stage_cb),
            ("Value ($)", ttk.Entry(self.form, textvariable=self.var_value)),
            ("Created (YYYY-MM-DD)", ttk.Entry(self.form, textvariable=self.var_created)),
            ("Close Date", ttk.Entry(self.form, textvariable=self.var_close)),
            ("Notes", self.txt_notes),
        ]:
            ttk.Label(self.form, text=label).grid(row=row, column=0, sticky=tk.W, pady=2)
            widget.grid(row=row, column=1, sticky=tk.EW, pady=2)
            row += 1
        self.form.grid_columnconfigure(1, weight=1)

        self.company_cb.bind("<<ComboboxSelected>>", self._on_company_change)

    def company_options(self):
        cur = self.conn.cursor()
        cur.execute("SELECT id, name FROM companies ORDER BY name")
        return [f"{r['id']} - {r['name']}" for r in cur.fetchall()]

    def contact_options(self, company_id):
        cur = self.conn.cursor()
        if company_id:
            cur.execute("SELECT id, name FROM contacts WHERE company_id=? ORDER BY name", (company_id,))
        else:
            cur.execute("SELECT id, name FROM contacts ORDER BY name")
        return [f"{r['id']} - {r['name']}" for r in cur.fetchall()]

    def _on_company_change(self, *_):
        cid = None
        try:
            cid = int(self.company_cb.get().split(" - ")[0])
        except Exception:
            pass
        self.contact_cb["values"] = self.contact_options(cid)

    def refresh(self):
        self.company_cb["values"] = self.company_options()
        self._on_company_change()
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT d.id, d.title, co.name as company, ct.name as contact, d.stage, d.value, d.created_at, COALESCE(d.close_date,'') close_date
            FROM deals d
            JOIN companies co ON d.company_id = co.id
            LEFT JOIN contacts ct ON d.contact_id = ct.id
            ORDER BY d.created_at DESC
            """
        )
        self.clear_tree()
        for r in cur.fetchall():
            self.tree.insert(
                "", tk.END, iid=r["id"],
                values=(r["id"], r["title"], r["company"], r["contact"] or "", r["stage"], f"{r['value']:.0f}", r["created_at"], r["close_date"])
            )

    def on_select(self):
        sel = self.tree.focus()
        if not sel:
            return
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM deals WHERE id=?", (sel,))
        r = cur.fetchone()
        if r:
            self.var_id.set(r["id"])
            self.var_title.set(r["title"])
            self.stage_cb.set(r["stage"])
            self.var_value.set(str(int(r["value"])) if r["value"] is not None else "0")
            self.var_created.set(r["created_at"] or "")
            self.var_close.set(r["close_date"] or "")
            self.txt_notes.delete("1.0", tk.END)
            self.txt_notes.insert(tk.END, r["notes"] or "")
            cur.execute("SELECT id, name FROM companies WHERE id=?", (r["company_id"],))
            cname = cur.fetchone()["name"]
            self.company_cb.set(f"{r['company_id']} - {cname}")
            self._on_company_change()
            if r["contact_id"]:
                cur.execute("SELECT name FROM contacts WHERE id=?", (r["contact_id"],))
                ctname = cur.fetchone()["name"]
                self.contact_cb.set(f"{r['contact_id']} - {ctname}")
            else:
                self.contact_cb.set("")

    def on_new(self):
        self.clear_form()
        self.var_id.set("")
        self.stage_cb.set(PIPELINE_STAGES[0])
        self.var_created.set(date.today().strftime(DATE_FMT))

    def on_save(self):
        title = self.var_title.get().strip()
        if not title:
            messagebox.showwarning(APP_NAME, "Deal title is required")
            return
        try:
            value = float(self.var_value.get().strip() or 0)
        except ValueError:
            messagebox.showwarning(APP_NAME, "Value must be a number")
            return
        stage = self.stage_cb.get() or PIPELINE_STAGES[0]
        created = self.var_created.get().strip() or date.today().strftime(DATE_FMT)
        close_d = self.var_close.get().strip() or None
        notes = self.txt_notes.get("1.0", tk.END).strip()

        company_id = None
        contact_id = None
        try:
            company_id = int(self.company_cb.get().split(" - ")[0])
        except Exception:
            pass
        if not company_id:
            messagebox.showwarning(APP_NAME, "Company is required")
            return
        if self.contact_cb.get():
            try:
                contact_id = int(self.contact_cb.get().split(" - ")[0])
            except Exception:
                contact_id = None

        cur = self.conn.cursor()
        if self.var_id.get():
            cur.execute(
                """
                UPDATE deals SET company_id=?, contact_id=?, title=?, value=?, stage=?, created_at=?, close_date=?, notes=?
                WHERE id=?
                """,
                (company_id, contact_id, title, value, stage, created, close_d, notes, self.var_id.get()),
            )
        else:
            cur.execute(
                """
                INSERT INTO deals(company_id, contact_id, title, value, stage, created_at, close_date, notes)
                VALUES(?,?,?,?,?,?,?,?)
                """,
                (company_id, contact_id, title, value, stage, created, close_d, notes),
            )
            self.var_id.set(cur.lastrowid)
        self.conn.commit()
        self.refresh()

    def on_delete(self):
        if not self.var_id.get():
            return
        if messagebox.askyesno(APP_NAME, "Delete this deal and its activities?"):
            cur = self.conn.cursor()
            cur.execute("DELETE FROM deals WHERE id=?", (self.var_id.get(),))
            self.conn.commit()
            self.on_new()
            self.refresh()


# -------------
# Activities
# -------------
class ActivitiesTab(ListFormBase):
    columns = [("ID", 50), ("Kind", 90), ("Due", 100), ("Done", 60), ("Company", 180), ("Deal", 220), ("Note", 340)]

    def build(self):
        super().build()
        self.var_id = tk.StringVar()
        self.kind_cb = ttk.Combobox(self.form, values=ACTIVITY_KINDS)
        self.var_due = tk.StringVar()
        self.var_done = tk.IntVar(value=0)
        self.company_cb = ttk.Combobox(self.form, values=self.company_options())
        self.deal_cb = ttk.Combobox(self.form, values=self.deal_options(None))
        self.txt_note = tk.Text(self.form, height=5, width=40)

        row = 0
        for label, widget in [
            ("ID", ttk.Entry(self.form, textvariable=self.var_id, state="readonly")),
            ("Kind", self.kind_cb),
            ("Due (YYYY-MM-DD)", ttk.Entry(self.form, textvariable=self.var_due)),
            ("Done", ttk.Checkbutton(self.form, variable=self.var_done)),
            ("Company", self.company_cb),
            ("Deal", self.deal_cb),
            ("Note", self.txt_note),
        ]:
            ttk.Label(self.form, text=label).grid(row=row, column=0, sticky=tk.W, pady=2)
            widget.grid(row=row, column=1, sticky=tk.EW, pady=2)
            row += 1
        self.form.grid_columnconfigure(1, weight=1)

        self.company_cb.bind("<<ComboboxSelected>>", self._on_company_change)

    def company_options(self):
        cur = self.conn.cursor()
        cur.execute("SELECT id, name FROM companies ORDER BY name")
        return [f"{r['id']} - {r['name']}" for r in cur.fetchall()]

    def deal_options(self, company_id):
        cur = self.conn.cursor()
        if company_id:
            cur.execute("SELECT id, title FROM deals WHERE company_id=? ORDER BY created_at DESC", (company_id,))
        else:
            cur.execute("SELECT id, title FROM deals ORDER BY created_at DESC")
        return [f"{r['id']} - {r['title']}" for r in cur.fetchall()]

    def _on_company_change(self, *_):
        cid = None
        try:
            cid = int(self.company_cb.get().split(" - ")[0])
        except Exception:
            pass
        self.deal_cb["values"] = self.deal_options(cid)

    def refresh(self):
        self.company_cb["values"] = self.company_options()
        self._on_company_change()
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT a.id, a.kind, COALESCE(a.due_date,'') due_date, a.done,
                   COALESCE(c.name,'') company, COALESCE(d.title,'') deal, COALESCE(a.note,'') note
            FROM activities a
            LEFT JOIN companies c ON a.company_id = c.id
            LEFT JOIN deals d ON a.deal_id = d.id
            ORDER BY a.due_date IS NULL, a.due_date
            """
        )
        self.clear_tree()
        for r in cur.fetchall():
            self.tree.insert(
                "", tk.END, iid=r["id"],
                values=(r["id"], r["kind"], r["due_date"], "✔" if r["done"] else "", r["company"], r["deal"], r["note"][:120])
            )

    def on_select(self):
        sel = self.tree.focus()
        if not sel:
            return
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM activities WHERE id=?", (sel,))
        r = cur.fetchone()
        if r:
            self.var_id.set(r["id"])
            self.kind_cb.set(r["kind"])
            self.var_due.set(r["due_date"] or "")
            self.var_done.set(int(r["done"]))
            if r["company_id"]:
                cur.execute("SELECT name FROM companies WHERE id=?", (r["company_id"],))
                cname = cur.fetchone()["name"]
                self.company_cb.set(f"{r['company_id']} - {cname}")
            else:
                self.company_cb.set("")
            if r["deal_id"]:
                cur.execute("SELECT title FROM deals WHERE id=?", (r["deal_id"],))
                dtitle = cur.fetchone()["title"]
                self.deal_cb.set(f"{r['deal_id']} - {dtitle}")
            else:
                self.deal_cb.set("")
            self.txt_note.delete("1.0", tk.END)
            self.txt_note.insert(tk.END, r["note"] or "")

    def on_new(self):
        self.clear_form()
        self.var_id.set("")
        self.kind_cb.set(ACTIVITY_KINDS[0])

    def on_save(self):
        kind = self.kind_cb.get() or ACTIVITY_KINDS[0]
        due = self.var_due.get().strip() or None
        done = int(self.var_done.get() or 0)
        note = self.txt_note.get("1.0", tk.END).strip()
        company_id = None
        deal_id = None
        try:
            company_id = int(self.company_cb.get().split(" - ")[0])
        except Exception:
            pass
        try:
            deal_id = int(self.deal_cb.get().split(" - ")[0])
        except Exception:
            pass
        cur = self.conn.cursor()
        if self.var_id.get():
            cur.execute(
                "UPDATE activities SET kind=?, note=?, due_date=?, done=?, company_id=?, deal_id=? WHERE id=?",
                (kind, note, due, done, company_id, deal_id, self.var_id.get()),
            )
        else:
            cur.execute(
                "INSERT INTO activities(kind, note, due_date, done, company_id, deal_id) VALUES(?,?,?,?,?,?)",
                (kind, note, due, done, company_id, deal_id),
            )
            self.var_id.set(cur.lastrowid)
        self.conn.commit()
        self.refresh()

    def on_delete(self):
        if not self.var_id.get():
            return
        if messagebox.askyesno(APP_NAME, "Delete this activity?"):
            cur = self.conn.cursor()
            cur.execute("DELETE FROM activities WHERE id=?", (self.var_id.get(),))
            self.conn.commit()
            self.on_new()
            self.refresh()


# -------------
# Time entries
# -------------
class TimeTab(ListFormBase):
    columns = [("ID", 50), ("Deal", 240), ("Date", 100), ("Hours", 80), ("Rate", 80), ("Notes", 320)]

    def build(self):
        super().build()
        self.var_id = tk.StringVar()
        self.deal_cb = ttk.Combobox(self.form, values=self.deal_options())
        self.var_date = tk.StringVar(value=date.today().strftime(DATE_FMT))
        self.var_hours = tk.StringVar(value="1.0")
        self.var_rate = tk.StringVar(value="150")
        self.txt_notes = tk.Text(self.form, height=4, width=40)

        row = 0
        for label, widget in [
            ("ID", ttk.Entry(self.form, textvariable=self.var_id, state="readonly")),
            ("Deal", self.deal_cb),
            ("Date (YYYY-MM-DD)", ttk.Entry(self.form, textvariable=self.var_date)),
            ("Hours", ttk.Entry(self.form, textvariable=self.var_hours)),
            ("Rate", ttk.Entry(self.form, textvariable=self.var_rate)),
            ("Notes", self.txt_notes),
        ]:
            ttk.Label(self.form, text=label).grid(row=row, column=0, sticky=tk.W, pady=2)
            widget.grid(row=row, column=1, sticky=tk.EW, pady=2)
            row += 1
        self.form.grid_columnconfigure(1, weight=1)

    def deal_options(self):
        cur = self.conn.cursor()
        cur.execute("SELECT id, title FROM deals ORDER BY created_at DESC")
        return [f"{r['id']} - {r['title']}" for r in cur.fetchall()]

    def refresh(self):
        self.deal_cb["values"] = self.deal_options()
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT t.id, d.title as deal, t.work_date, t.hours, t.rate, COALESCE(t.notes,'') notes
            FROM time_entries t JOIN deals d ON t.deal_id = d.id
            ORDER BY t.work_date DESC, t.id DESC
            """
        )
        self.clear_tree()
        for r in cur.fetchall():
            self.tree.insert("", tk.END, iid=r["id"], values=(r["id"], r["deal"], r["work_date"], r["hours"], r["rate"], r["notes"][:120]))

    def on_select(self):
        sel = self.tree.focus()
        if not sel:
            return
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM time_entries WHERE id=?", (sel,))
        r = cur.fetchone()
        if r:
            self.var_id.set(r["id"])
            self.var_date.set(r["work_date"])
            self.var_hours.set(str(r["hours"]))
            self.var_rate.set(str(r["rate"]))
            if r["deal_id"]:
                cur.execute("SELECT title FROM deals WHERE id=?", (r["deal_id"],))
                dtitle = cur.fetchone()["title"]
                self.deal_cb.set(f"{r['deal_id']} - {dtitle}")
            else:
                self.deal_cb.set("")
            self.txt_notes.delete("1.0", tk.END)
            self.txt_notes.insert(tk.END, r["notes"] or "")

    def on_new(self):
        self.clear_form()
        self.var_id.set("")
        self.var_date.set(date.today().strftime(DATE_FMT))
        self.var_hours.set("1.0")
        self.var_rate.set("150")

    def on_save(self):
        try:
            deal_id = int(self.deal_cb.get().split(" - ")[0])
        except Exception:
            messagebox.showwarning(APP_NAME, "Deal is required")
            return
        work_date = self.var_date.get().strip() or date.today().strftime(DATE_FMT)
        try:
            hours = float(self.var_hours.get().strip())
            rate = float(self.var_rate.get().strip())
        except ValueError:
            messagebox.showwarning(APP_NAME, "Hours and Rate must be numbers")
            return
        notes = self.txt_notes.get("1.0", tk.END).strip()
        cur = self.conn.cursor()
        if self.var_id.get():
            cur.execute(
                "UPDATE time_entries SET deal_id=?, work_date=?, hours=?, rate=?, notes=? WHERE id=?",
                (deal_id, work_date, hours, rate, notes, self.var_id.get()),
            )
        else:
            cur.execute(
                "INSERT INTO time_entries(deal_id, work_date, hours, rate, notes) VALUES(?,?,?,?,?)",
                (deal_id, work_date, hours, rate, notes),
            )
            self.var_id.set(cur.lastrowid)
        self.conn.commit()
        self.refresh()

    def on_delete(self):
        if not self.var_id.get():
            return
        if messagebox.askyesno(APP_NAME, "Delete this time entry?"):
            cur = self.conn.cursor()
            cur.execute("DELETE FROM time_entries WHERE id=?", (self.var_id.get(),))
            self.conn.commit()
            self.on_new()
            self.refresh()


# -------------
# Invoices
# -------------
class InvoicesTab(ListFormBase):
    columns = [("ID", 50), ("Deal", 240), ("Issue", 100), ("Due", 100), ("Status", 100), ("Notes", 320)]

    def build(self):
        super().build()
        self.var_id = tk.StringVar()
        self.deal_cb = ttk.Combobox(self.form, values=self.deal_options())
        self.var_issue = tk.StringVar(value=date.today().strftime(DATE_FMT))
        self.var_due = tk.StringVar()
        self.var_status = ttk.Combobox(self.form, values=["Draft", "Sent", "Paid", "Void"])
        self.var_status.set("Draft")
        self.txt_notes = tk.Text(self.form, height=4, width=40)

        # Items sub-frame
        ttk.Label(self.form, text="Items").grid(row=6, column=0, sticky=tk.W, pady=(10, 2))
        items_frame = ttk.Frame(self.form)
        items_frame.grid(row=6, column=1, sticky=tk.EW, pady=(10, 2))

        item_cols = ["Description", "Qty", "Unit"]
        self.items_tree = ttk.Treeview(items_frame, columns=item_cols, show="headings", height=5)
        for col, w in [("Description", 280), ("Qty", 60), ("Unit", 80)]:
            self.items_tree.heading(col, text=col)
            self.items_tree.column(col, width=w, anchor=tk.W)
        self.items_tree.pack(side=tk.LEFT, fill=tk.X, expand=True)

        items_vsb = ttk.Scrollbar(items_frame, orient="vertical", command=self.items_tree.yview)
        self.items_tree.configure(yscrollcommand=items_vsb.set)
        items_vsb.pack(side=tk.RIGHT, fill=tk.Y)

        items_btns = ttk.Frame(self.form)
        items_btns.grid(row=7, column=1, sticky=tk.W, pady=4)
        ttk.Button(items_btns, text="Add Item", command=self.add_item).pack(side=tk.LEFT)
        ttk.Button(items_btns, text="Edit Item", command=self.edit_item).pack(side=tk.LEFT, padx=6)
        ttk.Button(items_btns, text="Delete Item", command=self.delete_item).pack(side=tk.LEFT)

        # PDF button
        pdf_btns = ttk.Frame(self.form)
        pdf_btns.grid(row=8, column=1, sticky=tk.W, pady=(8, 0))
        ttk.Button(pdf_btns, text="Export PDF", command=self.export_pdf).pack(side=tk.LEFT)

        row = 0
        for label, widget in [
            ("ID", ttk.Entry(self.form, textvariable=self.var_id, state="readonly")),
            ("Deal", self.deal_cb),
            ("Issue (YYYY-MM-DD)", ttk.Entry(self.form, textvariable=self.var_issue)),
            ("Due (YYYY-MM-DD)", ttk.Entry(self.form, textvariable=self.var_due)),
            ("Status", self.var_status),
            ("Notes", self.txt_notes),
        ]:
            ttk.Label(self.form, text=label).grid(row=row, column=0, sticky=tk.W, pady=2)
            widget.grid(row=row, column=1, sticky=tk.EW, pady=2)
            row += 1
        self.form.grid_columnconfigure(1, weight=1)

    def deal_options(self):
        cur = self.conn.cursor()
        cur.execute("SELECT id, title FROM deals ORDER BY created_at DESC")
        return [f"{r['id']} - {r['title']}" for r in cur.fetchall()]

    def refresh(self):
        self.deal_cb["values"] = self.deal_options()
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT i.id, d.title as deal, i.issue_date, COALESCE(i.due_date,'') due_date, i.status, COALESCE(i.notes,'') notes
            FROM invoices i JOIN deals d ON i.deal_id = d.id
            ORDER BY i.issue_date DESC, i.id DESC
            """
        )
        self.clear_tree()
        for r in cur.fetchall():
            self.tree.insert("", tk.END, iid=r["id"], values=(r["id"], r["deal"], r["issue_date"], r["due_date"], r["status"], r["notes"][:120]))
        self.refresh_items_list()

    def on_select(self):
        sel = self.tree.focus()
        if not sel:
            return
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM invoices WHERE id=?", (sel,))
        r = cur.fetchone()
        if r:
            self.var_id.set(r["id"])
            self.var_issue.set(r["issue_date"])
            self.var_due.set(r["due_date"] or "")
            self.var_status.set(r["status"])
            if r["deal_id"]:
                cur.execute("SELECT title FROM deals WHERE id=?", (r["deal_id"],))
                dtitle = cur.fetchone()["title"]
                self.deal_cb.set(f"{r['deal_id']} - {dtitle}")
            else:
                self.deal_cb.set("")
            self.txt_notes.delete("1.0", tk.END)
            self.txt_notes.insert(tk.END, r["notes"] or "")
            self.refresh_items_list()

    def on_new(self):
        self.clear_form()
        self.var_id.set("")
        self.var_issue.set(date.today().strftime(DATE_FMT))
        self.var_status.set("Draft")
        self.items_tree.delete(*self.items_tree.get_children())

    def on_save(self):
        try:
            deal_id = int(self.deal_cb.get().split(" - ")[0])
        except Exception:
            messagebox.showwarning(APP_NAME, "Deal is required")
            return
        issue = self.var_issue.get().strip() or date.today().strftime(DATE_FMT)
        due = self.var_due.get().strip() or None
        status = self.var_status.get() or "Draft"
        notes = self.txt_notes.get("1.0", tk.END).strip()
        cur = self.conn.cursor()
        if self.var_id.get():
            cur.execute(
                "UPDATE invoices SET deal_id=?, issue_date=?, due_date=?, status=?, notes=? WHERE id=?",
                (deal_id, issue, due, status, notes, self.var_id.get()),
            )
        else:
            cur.execute(
                "INSERT INTO invoices(deal_id, issue_date, due_date, status, notes) VALUES(?,?,?,?,?)",
                (deal_id, issue, due, status, notes),
            )
            self.var_id.set(cur.lastrowid)
        self.conn.commit()
        self.refresh()

    def on_delete(self):
        if not self.var_id.get():
            return
        if messagebox.askyesno(APP_NAME, "Delete this invoice and its items?"):
            cur = self.conn.cursor()
            cur.execute("DELETE FROM invoices WHERE id=?", (self.var_id.get(),))
            self.conn.commit()
            self.on_new()
            self.refresh()

    # ----- items helpers -----
    def refresh_items_list(self):
        self.items_tree.delete(*self.items_tree.get_children())
        if not self.var_id.get():
            return
        cur = self.conn.cursor()
        cur.execute("SELECT id, description, qty, unit_price FROM invoice_items WHERE invoice_id=? ORDER BY id", (self.var_id.get(),))
        for r in cur.fetchall():
            self.items_tree.insert("", tk.END, iid=r["id"], values=(r["description"], r["qty"], r["unit_price"]))

    def add_item(self):
        if not self.var_id.get():
            messagebox.showinfo(APP_NAME, "Save the invoice first, then add items.")
            return
        desc = simpledialog.askstring(APP_NAME, "Description:", parent=self)
        if not desc:
            return
        q = simpledialog.askstring(APP_NAME, "Quantity:", initialvalue="1", parent=self)
        p = simpledialog.askstring(APP_NAME, "Unit price:", initialvalue="0", parent=self)
        try:
            qty = float(q or 1)
            price = float(p or 0)
        except ValueError:
            messagebox.showwarning(APP_NAME, "Qty and Unit price must be numbers")
            return
        cur = self.conn.cursor()
        cur.execute("INSERT INTO invoice_items(invoice_id, description, qty, unit_price) VALUES(?,?,?,?)", (self.var_id.get(), desc, qty, price))
        self.conn.commit()
        self.refresh_items_list()

    def edit_item(self):
        sel = self.items_tree.focus()
        if not sel:
            return
        cur = self.conn.cursor()
        cur.execute("SELECT description, qty, unit_price FROM invoice_items WHERE id=?", (sel,))
        r = cur.fetchone()
        if not r:
            return
        desc = simpledialog.askstring(APP_NAME, "Description:", initialvalue=r["description"], parent=self)
        if desc is None:
            return
        q = simpledialog.askstring(APP_NAME, "Quantity:", initialvalue=str(r["qty"]), parent=self)
        p = simpledialog.askstring(APP_NAME, "Unit price:", initialvalue=str(r["unit_price"]), parent=self)
        try:
            qty = float(q or 1)
            price = float(p or 0)
        except ValueError:
            messagebox.showwarning(APP_NAME, "Qty and Unit price must be numbers")
            return
        cur.execute("UPDATE invoice_items SET description=?, qty=?, unit_price=? WHERE id=?", (desc, qty, price, sel))
        self.conn.commit()
        self.refresh_items_list()

    def delete_item(self):
        sel = self.items_tree.focus()
        if not sel:
            return
        if messagebox.askyesno(APP_NAME, "Delete this item?"):
            cur = self.conn.cursor()
            cur.execute("DELETE FROM invoice_items WHERE id=?", (sel,))
            self.conn.commit()
            self.refresh_items_list()

    def export_pdf(self):
        if not HAVE_REPORTLAB:
            messagebox.showinfo(APP_NAME, "ReportLab not installed. Install with:\n\npip install reportlab")
            return
        if not self.var_id.get():
            messagebox.showinfo(APP_NAME, "Save the invoice first.")
            return

        inv_id = int(self.var_id.get())
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT i.id, i.issue_date, i.due_date, i.status, i.notes, d.title deal, c.name company
            FROM invoices i
            JOIN deals d ON i.deal_id = d.id
            JOIN companies c ON d.company_id = c.id
            WHERE i.id=?
            """, (inv_id,)
        )
        inv = cur.fetchone()
        cur.execute("SELECT description, qty, unit_price FROM invoice_items WHERE invoice_id=? ORDER BY id", (inv_id,))
        items = cur.fetchall()

        # Sum billable hours (roll-up) — optional: you can pre-fill items from time entries in future
        cur.execute("SELECT SUM(hours*rate) as total FROM time_entries WHERE deal_id=(SELECT deal_id FROM invoices WHERE id=?)", (inv_id,))
        roll_total = cur.fetchone()["total"] or 0.0

        path = filedialog.asksaveasfilename(defaultextension=".pdf", filetypes=[("PDF", "*.pdf")], title="Export Invoice PDF")
        if not path:
            return

        c = pdf_canvas.Canvas(path, pagesize=LETTER)
        width, height = LETTER
        y = height - 50
        c.setFont("Helvetica-Bold", 16)
        c.drawString(50, y, f"Invoice #{inv['id']}  -  {inv['company']}")
        y -= 24
        c.setFont("Helvetica", 10)
        c.drawString(50, y, f"Issue: {inv['issue_date']}    Due: {inv['due_date'] or '-'}    Status: {inv['status']}")
        y -= 16
        c.drawString(50, y, f"Deal: {inv['deal']}")
        y -= 24
        c.setFont("Helvetica-Bold", 12)
        c.drawString(50, y, "Items")
        y -= 18
        c.setFont("Helvetica", 10)
        total = 0.0
        for it in items:
            line = f"{it['description']}   x{it['qty']} @ {it['unit_price']:.2f}"
            c.drawString(60, y, line)
            line_total = (it["qty"] or 0) * (it["unit_price"] or 0)
            c.drawRightString(width - 60, y, f"{line_total:.2f}")
            total += line_total
            y -= 14
            if y < 80:
                c.showPage()
                y = height - 50
        if roll_total:
            c.setFont("Helvetica-Oblique", 10)
            c.drawString(60, y, "Billable hours (roll-up from Time tab)")
            c.drawRightString(width - 60, y, f"{roll_total:.2f}")
            total += roll_total
            y -= 16

        y -= 8
        c.setFont("Helvetica-Bold", 12)
        c.drawRightString(width - 60, y, f"TOTAL: {total:.2f}")
        y -= 24
        c.setFont("Helvetica", 9)
        if inv["notes"]:
            c.drawString(50, y, "Notes:")
            y -= 12
            for line in inv["notes"].splitlines():
                c.drawString(60, y, line[:100])
                y -= 12
                if y < 80:
                    c.showPage()
                    y = height - 50

        c.showPage()
        c.save()
        messagebox.showinfo(APP_NAME, f"Invoice PDF saved to\n{path}")


# -------------
# Main App
# -------------
class GateMiniCRMApp(ttk.Frame):
    def __init__(self, master):
        super().__init__(master)
        self.master.title(APP_NAME)
        self.master.geometry("1180x760")
        self.pack(fill=tk.BOTH, expand=True)

        self.conn = connect_db()

        # Optional change tracking (for sync)
        if ensure_change_tracking:
            ensure_change_tracking(self.conn)

        self.create_menu()
        self.create_header()
        self.create_tabs()
        self.refresh_all()

    def create_menu(self):
        menubar = tk.Menu(self.master)
        filemenu = tk.Menu(menubar, tearoff=0)
        filemenu.add_command(label="Export Companies CSV", command=self.export_companies)
        filemenu.add_command(label="Export Contacts CSV", command=self.export_contacts)
        filemenu.add_command(label="Export Deals CSV", command=self.export_deals)
        filemenu.add_command(label="Export Activities CSV", command=self.export_activities)
        filemenu.add_separator()
        filemenu.add_command(label="Import Companies CSV", command=lambda: ask_import_csv("companies", self.conn))
        filemenu.add_command(label="Import Contacts CSV", command=lambda: ask_import_csv("contacts", self.conn))
        filemenu.add_separator()
        filemenu.add_command(label="Quit", command=self.master.quit)
        menubar.add_cascade(label="File", menu=filemenu)

        syncmenu = tk.Menu(menubar, tearoff=0)
        syncmenu.add_command(label="Set Device ID / Invoice Prefix", command=lambda: init_device_settings_dialog(self.conn, self.master) if init_device_settings_dialog else None)
        syncmenu.add_separator()
        syncmenu.add_command(label="Export Sync Pack…", command=lambda: export_changes_dialog(self.conn, self.master) if export_changes_dialog else None)
        syncmenu.add_command(label="Import Sync Pack…", command=lambda: import_pack_dialog(self.conn, self.master) if import_pack_dialog else None)
        menubar.add_cascade(label="Sync", menu=syncmenu)

        self.master.config(menu=menubar)

    def create_header(self):
        top = ttk.Frame(self)
        top.pack(fill=tk.X, padx=8, pady=(8, 0))
        ttk.Label(top, text="Pipeline Snapshot", font=("Segoe UI", 12, "bold")).pack(side=tk.LEFT)
        self.pipeline_var = tk.StringVar(value="…")
        ttk.Label(top, textvariable=self.pipeline_var).pack(side=tk.LEFT, padx=12)
        ttk.Button(top, text="Refresh", command=self.refresh_all).pack(side=tk.RIGHT)

    def create_tabs(self):
        nb = ttk.Notebook(self)
        nb.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)

        self.tab_companies = CompaniesTab(nb, self.conn)
        self.tab_contacts = ContactsTab(nb, self.conn)
        self.tab_deals = DealsTab(nb, self.conn)
        self.tab_acts = ActivitiesTab(nb, self.conn)
        self.tab_time = TimeTab(nb, self.conn)
        self.tab_invoices = InvoicesTab(nb, self.conn)

        nb.add(self.tab_companies, text="Companies")
        nb.add(self.tab_contacts, text="Contacts")
        nb.add(self.tab_deals, text="Deals")
        nb.add(self.tab_acts, text="Activities / Notes")
        nb.add(self.tab_time, text="Time (Billable)")
        nb.add(self.tab_invoices, text="Invoices")

    def refresh_all(self):
        for t in (self.tab_companies, self.tab_contacts, self.tab_deals, self.tab_acts, self.tab_time, self.tab_invoices):
            t.refresh()
        cur = self.conn.cursor()
        cur.execute("SELECT stage, COUNT(*) c, COALESCE(SUM(value),0) v FROM deals GROUP BY stage")
        parts = [f"{row['stage']}: {row['c']} ($ {row['v']:.0f})" for row in cur.fetchall()]
        self.pipeline_var.set("  |  ".join(parts) if parts else "No deals yet")

    # Export helpers
    def export_companies(self):
        cur = self.conn.cursor()
        cur.execute("SELECT id, name, phone, email, website, address, created_at FROM companies ORDER BY name")
        rows = [dict(r) for r in cur.fetchall()]
        ask_export_csv(list(rows[0].keys()) if rows else ["id","name","phone","email","website","address","created_at"], rows)

    def export_contacts(self):
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT c.id, c.name, c.title, c.email, c.phone,
                   coalesce(co.name,'') as company
            FROM contacts c LEFT JOIN companies co ON c.company_id = co.id
            ORDER BY c.name
            """
        )
        rows = [dict(r) for r in cur.fetchall()]
        ask_export_csv(list(rows[0].keys()) if rows else ["id","name","title","email","phone","company"], rows)

    def export_deals(self):
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT d.id, d.title, d.value, d.stage, d.created_at, d.close_date,
                   coalesce(co.name,'') as company,
                   coalesce(ct.name,'') as contact
            FROM deals d
            JOIN companies co ON d.company_id = co.id
            LEFT JOIN contacts ct ON d.contact_id = ct.id
            ORDER BY d.created_at DESC
            """
        )
        rows = [dict(r) for r in cur.fetchall()]
        ask_export_csv(list(rows[0].keys()) if rows else ["id","title","value","stage","created_at","close_date","company","contact"], rows)

    def export_activities(self):
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT a.id, a.kind, a.note, a.due_date, a.done,
                   coalesce(d.title,'') as deal,
                   coalesce(c.name,'') as company
            FROM activities a
            LEFT JOIN deals d ON a.deal_id = d.id
            LEFT JOIN companies c ON a.company_id = c.id
            ORDER BY a.due_date IS NULL, a.due_date
            """
        )
        rows = [dict(r) for r in cur.fetchall()]
        ask_export_csv(list(rows[0].keys()) if rows else ["id","kind","note","due_date","done","deal","company"], rows)


# -------------
# app entry
# -------------
def main():
    init_db()
    root = tk.Tk()
    # Native look
    try:
        s = ttk.Style()
        if root.tk.call("tk", "windowingsystem") == "aqua":
            s.theme_use("aqua")
        else:
            s.theme_use("clam")
    except Exception:
        pass

    # Ensure passcode before opening main UI
    conn = connect_db()
    ok = ensure_passcode(conn, root)
    conn.close()
    if not ok:
        root.destroy()
        return

    app = GateMiniCRMApp(root)
    app.mainloop()


if __name__ == "__main__":
    main()
