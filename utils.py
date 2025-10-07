def fmt_price(x: float) -> str:
    return f"{x:.1f}"

def risk_summary(side: str, entry: float, sl: float, tp: float) -> str:
    if side == "LONG":
        rr = (tp - entry) / max(entry - sl, 1e-12)
    else:
        rr = (entry - tp) / max(sl - entry, 1e-12)
    return f"R:R ≈ {rr:.2f}"
