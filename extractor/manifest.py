def build_manifest(rows: list[dict], file_name: str) -> dict:
    if not rows:
        return {"file_name": file_name, "trading_date": None, "row_count": 0}
    return {
        "file_name": file_name,
        "trading_date": rows[0]["trading_date"],
        "row_count": len(rows),
    }
