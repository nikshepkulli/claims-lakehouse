"""Small reference vocabularies. Code *formats* are real (CPT/ICD-10 shaped);
all descriptions and usage are fabricated for synthetic data."""

from __future__ import annotations

import re

import numpy as np

# (code, base_cost_usd) - ordered roughly by frequency (zipf weighted below).
PROCEDURES: list[tuple[str, float]] = [
    ("99213", 110), ("99214", 165), ("99212", 75), ("99203", 150), ("99204", 230),
    ("36415", 12), ("80053", 35), ("85025", 28), ("83036", 30), ("93000", 55),
    ("71046", 95), ("97110", 60), ("97140", 55), ("90834", 140), ("90837", 190),
    ("99283", 380), ("99284", 620), ("99285", 980), ("99221", 260), ("99232", 150),
    ("72148", 1100), ("73721", 1050), ("70553", 1900), ("74177", 1500), ("45378", 1700),
    ("43239", 1300), ("29881", 6200), ("27447", 18500), ("33533", 41000), ("47562", 9800),
    ("59400", 5200), ("59510", 7800), ("66984", 3900), ("92014", 130), ("99395", 190),
    ("G0008", 25), ("J1745", 900), ("J9035", 3200), ("A0427", 1800), ("E0601", 700),
]
PROC_CODES = np.array([c for c, _ in PROCEDURES])
PROC_BASE = np.array([b for _, b in PROCEDURES], dtype=np.float64)
INVALID_PROC = ["ABCDE", "99-99", "0000", "12", "XXXXX", "7O213"]

# (icd10, description, complaint phrase for notes)
DIAGNOSES: list[tuple[str, str, str]] = [
    ("I10", "essential hypertension", "elevated blood pressure readings"),
    ("E11.9", "type 2 diabetes without complications", "routine diabetes follow-up"),
    ("E78.5", "hyperlipidemia, unspecified", "lipid panel review"),
    ("J06.9", "acute upper respiratory infection", "sore throat and congestion"),
    ("J45.909", "unspecified asthma, uncomplicated", "wheezing and shortness of breath"),
    ("M54.5", "low back pain", "lower back pain for two weeks"),
    ("F41.9", "anxiety disorder, unspecified", "persistent anxiety and poor sleep"),
    ("F32.9", "major depressive disorder, single episode", "low mood and fatigue"),
    ("K21.9", "gastro-esophageal reflux disease", "heartburn after meals"),
    ("N39.0", "urinary tract infection", "dysuria and urgency"),
    ("R07.9", "chest pain, unspecified", "intermittent chest discomfort"),
    ("R10.9", "abdominal pain, unspecified", "abdominal pain"),
    ("R51.9", "headache, unspecified", "recurrent headaches"),
    ("J18.9", "pneumonia, unspecified organism", "productive cough and fever"),
    ("I25.10", "atherosclerotic heart disease", "exertional chest tightness"),
    ("I50.9", "heart failure, unspecified", "ankle swelling and dyspnea"),
    ("N18.3", "chronic kidney disease, stage 3", "creatinine trend review"),
    ("M17.9", "osteoarthritis of knee, unspecified", "knee pain limiting walking"),
    ("S83.511A", "sprain of anterior cruciate ligament, right knee", "knee injury"),
    ("C50.911", "malignant neoplasm of right female breast", "oncology follow-up"),
    ("O80", "encounter for full-term uncomplicated delivery", "labor and delivery"),
    ("Z00.00", "general adult medical examination", "annual wellness visit"),
    ("Z23", "encounter for immunization", "seasonal vaccination"),
    ("G47.33", "obstructive sleep apnea", "snoring and daytime sleepiness"),
    ("E66.9", "obesity, unspecified", "weight management consult"),
    ("L30.9", "dermatitis, unspecified", "itchy skin rash"),
    ("H66.90", "otitis media, unspecified", "ear pain"),
    ("B34.9", "viral infection, unspecified", "fever and malaise"),
    ("K35.80", "acute appendicitis", "acute right lower quadrant pain"),
    ("I48.91", "unspecified atrial fibrillation", "palpitations"),
]
ICD_CODES = np.array([c for c, _, _ in DIAGNOSES])
ICD_DESC = [d for _, d, _ in DIAGNOSES]
ICD_COMPLAINT = [p for _, _, p in DIAGNOSES]
INVALID_ICD = ["123.45", "??.1", "I10-9", "ZZZ", "99999", "i1o"]

# Validation regexes used by tests (and recommended for downstream quality rules).
PROC_RE = re.compile(r"^(\d{5}|[A-V]\d{4})$")
ICD_RE = re.compile(r"^[A-TV-Z]\d[0-9A-Z](\.[0-9A-Z]{1,4})?$")

SPECIALTIES = ["Family Medicine", "Internal Medicine", "Cardiology", "Orthopedics",
               "Emergency Medicine", "Radiology", "Oncology", "Pediatrics",
               "Behavioral Health", "General Surgery", "OB/GYN", "Physical Therapy"]
STATES = ["CA", "TX", "NY", "FL", "IL", "PA", "OH", "GA", "NC", "MI", "WA", "AZ", "MA", "CO", "VA"]
PLANS = ["HMO", "PPO", "EPO", "HDHP", "MEDICARE_ADV", "MEDICAID_MCO"]
PLAN_P = [0.25, 0.30, 0.10, 0.15, 0.12, 0.08]
CLAIM_TYPES = ["PROFESSIONAL", "INSTITUTIONAL", "PHARMACY"]
CLAIM_TYPE_P = [0.60, 0.25, 0.15]
POS_BY_TYPE = {0: ["11", "22", "23", "81"], 1: ["21", "22", "23", "31"], 2: ["01"]}


def zipf_weights(n: int, s: float) -> np.ndarray:
    w = 1.0 / np.arange(1, n + 1) ** s
    return w / w.sum()


PROC_W = zipf_weights(len(PROCEDURES), 0.9)
ICD_W = zipf_weights(len(DIAGNOSES), 0.8)
