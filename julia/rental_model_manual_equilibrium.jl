
using CSV, DataFrames

# Matching function
function g(x)
    return x < 1e-8 ? 1.0 : (1 - exp(-x)) / x
end

# Model parameters (can replace with CSV input)
low_landlord_cost = 0
high_landlord_cost = 0
tenant_high_share = 0.9   # s
market_tightness = 0.7   # n = tenants / landlords
high_landlord_share = 0.9 # h
valuation = 3
h = 0.5  # share of high landlords
n = 1.0  # market tightness
s_vals = 0.0:0.1:1.0

# Store results
results = DataFrame(s=Float64[], r_H=Float64[], r_L=Float64[], profit_H=Float64[], profit_L=Float64[])

# Manual grid over rents and queue lengths
r_grid = 2.0:0.2:10.0
x_grid = 0.1:0.1:5.0

for s in s_vals
    best_loss = Inf
    best_outcome = nothing

    for r_H in r_grid, r_L in r_grid
        for xHh in x_grid, xHl in x_grid, xLh in x_grid, xLl in x_grid
            # Match rates
            qHh = g(xHh)
            qHl = exp(-xHh) * g(xHl)
            qLh = g(xLh)
            qLl = g(xLl)

            # Utilities
            U_h = qHh*(10 - r_H)*h*xHh/(s*n) + qLh*(10 - r_L)*(1-h)*xLh/(s*n)
            U_l = qHl*(10 - r_H)*h*xHl/((1-s)*n) + qLl*(10 - r_L)*(1-h)*xLl/((1-s)*n)

            # Profit
            pi_H = qHh*r_H*xHh + qHl*r_H*xHl - high_landlord_cost
            pi_L = qLh*r_L*xLh + qLl*r_L*xLl - low_landlord_cost

            # Simple loss function (e.g., U_h ≈ U_l → indifference)
            loss = (U_h - U_l)^2

            if loss < best_loss
                best_loss = loss
                best_outcome = (r_H=r_H, r_L=r_L, pi_H=pi_H, pi_L=pi_L)
            end
        end
    end

    push!(results, (
        s = s,
        r_H = best_outcome.r_H,
        r_L = best_outcome.r_L,
        profit_H = best_outcome.pi_H,
        profit_L = best_outcome.pi_L
    ))
end


