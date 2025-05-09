#!/usr/bin/env python3
"""
Simple script to test CapEx guide printing
"""

print("Debug: Starting simple CapEx guide script...")

# CapEx Components with typical lifespans and costs
CAPEX_COMPONENTS = {
    "roof": {"lifespan": 25, "cost_per_sqft": 5.5},
    "hvac": {"lifespan": 18, "cost_base": 4500, "cost_per_sqft": 1.5},
    "water_heater": {"lifespan": 10, "cost_base": 900},
    "electrical": {"lifespan": 35, "cost_base": 1800}
}

# Property condition multipliers - affects maintenance and CapEx costs
CONDITION_MULTIPLIERS = {
    "excellent": 0.7,  # Lower costs for excellent condition
    "good": 1.0,       # Baseline
    "fair": 1.3,       # Higher costs for fair condition
    "poor": 1.7        # Much higher costs for poor condition
}

print("\n" + "=" * 80)
print(f"CAPEX COMPONENTS REFERENCE GUIDE")
print("=" * 80)
print("This guide shows typical CapEx components, their default lifespans and costs.")
print("=" * 80)

print(f"{'Component':<20} {'Typical Lifespan':<20} {'Cost Basis':<30}")
print("-" * 80)

for component, details in CAPEX_COMPONENTS.items():
    component_name = component.replace('_', ' ').title()
    lifespan = f"{details['lifespan']} years"
    
    if "cost_per_sqft" in details:
        cost_basis = f"${details['cost_per_sqft']:.2f}/sq ft"
        if "cost_base" in details:
            cost_basis += f" + ${details['cost_base']:.2f} base"
    else:
        cost_basis = f"${details['cost_base']:.2f} flat fee"
        
    print(f"{component_name:<20} {lifespan:<20} {cost_basis:<30}")

print("=" * 80)
print("PROPERTY CONDITION MULTIPLIERS")
print("-" * 80)
for condition, multiplier in sorted(CONDITION_MULTIPLIERS.items()):
    print(f"{condition.title():<20} {multiplier:.2f}x")

print("=" * 80)
print("Debug: Script completed successfully.")
