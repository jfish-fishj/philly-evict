using Optim

# === Landlord Profit Functions ===
function profit_high_landlord(r_H, xHH, xHL, fc=fc_h)
    total_queue = xHH + xHL
    prob_match = 1 - exp(-total_queue)
    return r_H * prob_match - fc
end

function profit_low_landlord(r_L, xLH, xLL, fc=fc_l)
    total_queue = xLH + xLL
    prob_match = 1 - exp(-total_queue)
    return r_L * prob_match - fc
end

# === Rent Optimization ===
function optimize_r_H(r_L, s, n, h, valuation, fc_h)
    obj(r_H) = begin
        try
            x = inner_solve(r_H, r_L, s, n, h, valuation)
            return -profit_high_landlord(r_H, x[1], x[2], fc_h)
        catch
            return 1e6
        end
    end
    result = optimize(obj, 1e-3, valuation - 1e-3, Brent())
    return Optim.minimizer(result)
end

function optimize_r_L(r_H, s, n, h, valuation, fc_l)
    obj(r_L) = begin
        try
            x = inner_solve(r_H, r_L, s, n, h, valuation)
            return -profit_low_landlord(r_L, x[3], x[4], fc_l)
        catch
            return 1e6
        end
    end
    result = optimize(obj, 1e-3, valuation - 1e-3, Brent())
    return Optim.minimizer(result)
end

# === Main Fixed Point Solver ===
function fixed_point_rent_solver(initial_r_H, initial_r_L; tol=1e-4, maxiter=20)
    r_H, r_L = initial_r_H, initial_r_L
    x = inner_solve(r_H, r_L, s, n, h, valuation)

    for iter in 1:maxiter
        println("\n🔁 Iteration $iter")
        
        r_H_new = optimize_r_H(r_L, s, n, h, valuation, fc_h)
        x = inner_solve(r_H_new, r_L, s, n, h, valuation)
        r_L_new = optimize_r_L(r_H_new, s, n, h, valuation, fc_l)

        println("r_H = $(round(r_H_new, digits=3)), r_L = $(round(r_L_new, digits=3))")

        if abs(r_H_new - r_H) < tol && abs(r_L_new - r_L) < tol
            println("✅ Converged at iteration $iter")
            return r_H_new, r_L_new, x
        end

        r_H, r_L = r_H_new, r_L_new
    end

    println("⚠️ Max iterations reached")
    return r_H, r_L, x
end

valuation = 100.0
s = 0.1     # Share of high-type tenants
n = 0.9     # Tenant to landlord ratio
h = 0.5     # Share of high-type landlords
fc_h = 0.0  # High-type fixed cost
fc_l = 0.0  # Low-type fixed cost

initial_r_H = valuation / 2
initial_r_L = valuation / 3

r_H, r_L, x = fixed_point_rent_solver(initial_r_H, initial_r_L)


using ForwardDiff
using FiniteDiff

function high_landlord_profit(r_H, r_L, s, n, h, v)
    x = inner_solve(r_H, r_L, s, n, h, v)
    xHH, xHL = x[1], x[2]
    match_prob = 1 - exp(-(xHH + xHL))
    return r_H * match_prob - fc_h
end

function low_landlord_profit(r_H, r_L, s, n, h, v)
    x = inner_solve(r_H, r_L, s, n, h, v)
    xLH, xLL = x[3], x[4]
    match_prob = 1 - exp(-(xLH + xLL))
    return r_L * match_prob - fc_l
end
using FiniteDiff
# Derivatives w.r.t. rent
v = valuation
r_H_guess = valuation / 2
r_L_guess = valuation / 2
r_L_fixed = r_L_guess
r_H_fixed = r_H_guess
step_size = valuation / 50  # Adjust step size based on valuation
rL_grid = r_H / 2:step_size:valuation  # Adjust rH_grid based on r_L
rH_grid = r_L / 2:step_size:valuation  # Adjust rL_grid based on r_H


dπH_drH = FiniteDiff.finite_difference_derivative(
    r -> high_landlord_profit(r, r_L_fixed, s, n, h, v),
    r_H_guess
)

dπL_drL = FiniteDiff.finite_difference_derivative(r -> low_landlord_profit(r,r_H_fixed, s, n, h, v), 
r_L_guess)