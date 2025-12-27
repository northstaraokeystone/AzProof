"""
ESA Transaction Category Classification Module

Classifies transactions as educational vs non-educational.
"""

from typing import Any, Dict, List, Optional

from ..core import emit_receipt, TENANT_ID, ESA_EGREGIOUS_KEYWORDS


# Merchant Category Code (MCC) mappings
# Educational categories (allowlist)
EDUCATIONAL_MCCS = {
    "8211": "elementary_secondary_schools",
    "8220": "colleges_universities",
    "8241": "correspondence_schools",
    "8244": "business_secretarial_schools",
    "8249": "vocational_trade_schools",
    "8299": "schools_educational_services",
    "5942": "book_stores",
    "5943": "stationery_stores",
    "5944": "jewelry_stores",  # Educational if < threshold
    "5045": "computers_peripherals",
    "5732": "electronics_stores",
    "5734": "computer_software_stores",
    "7392": "consulting_services",
    "7399": "business_services",  # Tutoring services
    "8011": "medical_services",  # Therapy if documented
    "8031": "osteopaths",
    "8041": "chiropractors",
    "8042": "optometrists",
    "8043": "opticians",
    "8049": "podiatrists",
    "8050": "nursing_care",
    "8062": "hospitals",
    "8099": "health_practitioners",
}

# Non-educational categories (denylist with evidence)
NON_EDUCATIONAL_MCCS = {
    "7011": "lodging",
    "7012": "timeshares",
    "7032": "recreational_camps",
    "7033": "campgrounds",
    "7941": "sports_clubs",
    "7991": "tourist_attractions",
    "7992": "golf_courses",
    "7993": "video_games",
    "7994": "video_game_arcades",
    "7995": "gambling",
    "7996": "amusement_parks",
    "7997": "country_clubs",
    "7998": "aquariums",
    "7999": "recreation_services",
    "5813": "bars",
    "5814": "fast_food",
    "5921": "liquor_stores",
    "5931": "used_merchandise",
    "5932": "antique_shops",
    "5933": "pawn_shops",
    "5935": "wrecking_salvage",
    "5937": "antique_reproductions",
    "5940": "bicycle_shops",
    "5941": "sporting_goods",
    "5945": "hobby_toy_game",
    "5946": "camera_photo",
    "5947": "gift_card_novelty",
    "5948": "luggage",
    "5949": "sewing_fabric",
    "5950": "glassware",
    "5960": "direct_marketing",
    "5961": "mail_order",
    "5962": "travel_agencies",
    "5963": "door_to_door",
    "5964": "catalog_merchant",
    "5965": "combination_catalog",
    "5966": "outbound_telemarketing",
    "5967": "inbound_telemarketing",
    "5968": "subscription_merchant",
    "5969": "direct_marketers",
    "5970": "artist_supply",
    "5971": "art_dealers",
    "5972": "stamp_coin",
    "5973": "religious_goods",
    "5975": "hearing_aids",
    "5976": "orthopedic",
    "5977": "cosmetics",
    "5978": "typewriter_stores",
    "5983": "fuel_dealers",
    "5992": "florists",
    "5993": "cigar_stores",
    "5994": "news_dealers",
    "5995": "pet_shops",
    "5996": "swimming_pools",
    "5997": "electric_razor",
    "5998": "tent_awning",
    "5999": "miscellaneous_retail",
}

# Known non-educational merchant patterns
NON_EDUCATIONAL_MERCHANTS = [
    "snowbowl",
    "ski resort",
    "ski lift",
    "trampoline",
    "ninja",
    "bounce",
    "arcade",
    "casino",
    "bar",
    "brewery",
    "winery",
    "tavern",
    "pub",
    "nightclub",
    "strip club",
    "adult",
    "tobacco",
    "vape",
    "cbd",
    "dispensary",
]


def load_category_rules() -> Dict[str, Any]:
    """
    Load MCC -> educational mapping.

    Returns:
        Dict with educational and non_educational MCC mappings
    """
    return {
        "educational": EDUCATIONAL_MCCS,
        "non_educational": NON_EDUCATIONAL_MCCS,
        "egregious_keywords": ESA_EGREGIOUS_KEYWORDS,
        "non_educational_merchants": NON_EDUCATIONAL_MERCHANTS
    }


def classify_transaction(txn: Dict[str, Any]) -> Dict[str, Any]:
    """
    Classify transaction as educational vs non-educational.

    Args:
        txn: Transaction dict

    Returns:
        Dict with category, confidence, and educational_flag
    """
    mcc = str(txn.get("merchant_category_code", "")).strip()
    merchant_name = str(txn.get("merchant_name", "")).lower()
    description = str(txn.get("description", "")).lower()
    amount = txn.get("amount", 0)

    # Default values
    category = "questionable"
    confidence = 0.5
    educational_flag = None
    reason = "unable_to_classify"

    # Check for egregious keywords first (highest priority)
    for keyword in ESA_EGREGIOUS_KEYWORDS:
        if keyword in merchant_name or keyword in description:
            category = "non_educational"
            confidence = 0.95
            educational_flag = False
            reason = f"egregious_keyword:{keyword}"
            break

    # Check non-educational merchant patterns
    if category == "questionable":
        for pattern in NON_EDUCATIONAL_MERCHANTS:
            if pattern in merchant_name:
                category = "non_educational"
                confidence = 0.90
                educational_flag = False
                reason = f"non_educational_merchant:{pattern}"
                break

    # Check MCC codes
    if category == "questionable":
        if mcc in NON_EDUCATIONAL_MCCS:
            category = "non_educational"
            confidence = 0.85
            educational_flag = False
            reason = f"non_educational_mcc:{mcc}"
        elif mcc in EDUCATIONAL_MCCS:
            category = "educational"
            confidence = 0.85
            educational_flag = True
            reason = f"educational_mcc:{mcc}"

    # Additional checks for common educational indicators
    if category == "questionable":
        educational_indicators = [
            "school", "academy", "learning", "tutor", "curriculum",
            "education", "college", "university", "textbook", "workbook"
        ]
        for indicator in educational_indicators:
            if indicator in merchant_name or indicator in description:
                category = "educational"
                confidence = 0.70
                educational_flag = True
                reason = f"educational_indicator:{indicator}"
                break

    return {
        "txn_id": txn.get("txn_id"),
        "category": category,
        "confidence": confidence,
        "educational_flag": educational_flag,
        "mcc": mcc,
        "merchant_name": txn.get("merchant_name"),
        "amount": amount,
        "reason": reason
    }


def detect_category_gaming(txns: List[Dict]) -> List[Dict]:
    """
    Flag attempts to miscategorize (e.g., "tutoring" at ski resort).

    Args:
        txns: List of transactions

    Returns:
        List of flagged gaming attempts
    """
    flagged = []

    for txn in txns:
        merchant_name = str(txn.get("merchant_name", "")).lower()
        description = str(txn.get("description", "")).lower()
        mcc = str(txn.get("merchant_category_code", ""))

        # Check for educational language with non-educational MCC
        educational_language = any(
            word in description for word in
            ["tutor", "lesson", "education", "curriculum", "learning", "class"]
        )

        is_non_educational_mcc = mcc in NON_EDUCATIONAL_MCCS
        is_non_educational_merchant = any(
            pattern in merchant_name for pattern in NON_EDUCATIONAL_MERCHANTS
        )

        if educational_language and (is_non_educational_mcc or is_non_educational_merchant):
            flagged.append({
                **txn,
                "gaming_type": "educational_language_non_educational_merchant",
                "evidence": {
                    "description_has_educational": educational_language,
                    "mcc_non_educational": is_non_educational_mcc,
                    "merchant_non_educational": is_non_educational_merchant
                }
            })

    return flagged


def compute_educational_ratio(account_id: str, txns: List[Dict]) -> float:
    """
    Percentage of spending on educational items.

    Args:
        account_id: Account to analyze
        txns: List of all transactions

    Returns:
        Ratio of educational spending (0.0 to 1.0)
    """
    account_txns = [
        t for t in txns
        if t.get("account_id") == account_id
    ]

    if not account_txns:
        return 0.0

    total_amount = 0.0
    educational_amount = 0.0

    for txn in account_txns:
        amount = txn.get("amount", 0)
        if amount <= 0:
            continue

        total_amount += amount

        classification = classify_transaction(txn)
        if classification.get("educational_flag"):
            educational_amount += amount

    if total_amount == 0:
        return 0.0

    return educational_amount / total_amount


def emit_category_receipt(txn: Dict, tenant_id: str = TENANT_ID) -> Dict:
    """
    Classify transaction and emit category receipt.

    Args:
        txn: Transaction to classify
        tenant_id: Tenant identifier

    Returns:
        Category receipt
    """
    classification = classify_transaction(txn)

    return emit_receipt("voucher_category", classification, tenant_id)
