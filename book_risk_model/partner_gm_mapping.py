#!/usr/bin/env python3
"""Authoritative Partner-GM mapping based on actual business relationships"""

def get_gm_from_partner(partner_name: str) -> str:
    """
    Get the correct GM based on partner name.
    This overrides any Area field mapping that might be wrong.
    """
    if not partner_name:
        return None

    # Authoritative Partner -> GM mappings based on business relationships
    partner_gm_map = {
        # Trent Hebert's Partners
        'enCOMPASS Agency': 'Trent Hebert',
        'Salem': 'Trent Hebert',
        'SalemSurround': 'Trent Hebert',
        'BFM Group': 'Trent Hebert',  # Corrected based on data validation
        'BFM Group Inc.': 'Trent Hebert',
        'Bokka Group': 'Trent Hebert',

        # Stephen Jones' Partners
        'Creekmore Marketing': 'Stephen Jones',
        'Creekmore Marketing, LLC': 'Stephen Jones',
        'Creekmore Marketing, LLC Invoice': 'Stephen Jones',
    }

    # Check exact matches first
    if partner_name in partner_gm_map:
        return partner_gm_map[partner_name]

    # Check partial matches for variations
    partner_lower = partner_name.lower()
    for partner_key, gm in partner_gm_map.items():
        if partner_key.lower() in partner_lower or partner_lower in partner_key.lower():
            return gm

    return None  # No mapping found


def correct_gm_by_partner(df, partner_column='partner_name', gm_column='gm'):
    """
    Correct GM assignments based on authoritative partner mappings.
    This fixes systemic issues where Area field has wrong GM assignments.
    """
    corrected_count = 0

    for idx, row in df.iterrows():
        partner = row.get(partner_column, '')
        current_gm = row.get(gm_column, '')

        # Get correct GM from partner mapping
        correct_gm = get_gm_from_partner(partner)

        if correct_gm and correct_gm != current_gm:
            df.at[idx, gm_column] = correct_gm
            corrected_count += 1
            print(f"   Partner override: {partner} -> {correct_gm} (was {current_gm})")

    if corrected_count > 0:
        print(f"Corrected {corrected_count} GM assignments using partner mapping")

    return df