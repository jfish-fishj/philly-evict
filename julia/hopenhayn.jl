#################### exit_model_stationary_se_fast.jl ####################
using Random, Distributions, Statistics, LinearAlgebra
using Base.Threads
using DataFrames, CSV, StatsBase
# (optional) keep BLAS from over-threading if you also use Threads.@threads
try
    BLAS.set_num_threads(min(nthreads(), 4))
catch
end

# =========================== Model Primitives =============================
struct Params
    β::Float64
    a::Float64
    b::Float64
    c̄::Float64
    ρx::Float64
    σx::Float64
end

# flow of services & costs
y(x) = 1
c(x, p::Params) = (p.c̄)
# === Productivity scales the price (quality shifter) ===
# alternatively, normalize explicitly:
# ϕ(x) = exp(γ * x)      # with γ ~ 1.0; or 
# heterogeneity / entry distributions
struct Hetero
    θdist::Distribution
    Sdist::Distribution
    x0dist::Distribution
end

# ====================== Rouwenhorst Discretization ========================
struct AR1Disc
    xgrid::Vector{Float64}
    P::Matrix{Float64}
end

function rouwenhorst(K::Int, ρ::Float64, σ::Float64; μ::Float64=0.0)
    @assert K ≥ 2
    @assert 0.0 < ρ < 0.999999

    p = (1 + ρ)/2
    q = p

    P = [p 1-p; 1-q q]
    for k in 3:K
        Pold = P
        TL = zeros(k, k);  TL[1:k-1, 1:k-1] = Pold
        TR = zeros(k, k);  TR[1:k-1, 2:k]   = Pold
        BL = zeros(k, k);  BL[2:k,   1:k-1] = Pold
        BR = zeros(k, k);  BR[2:k,   2:k]   = Pold
        P = p*TL + (1-p)*TR + (1-q)*BL + q*BR
        if k > 2
            P[2:end-1, :] .*= 0.5
        end
    end

    σ∞ = σ / sqrt(1 - ρ^2)
    span = σ∞ * sqrt(K - 1)
    xgrid = collect(range(μ - span, μ + span; length=K))

    @assert all(abs.(sum(P, dims=2) .- 1) .< 1e-12)
    @assert all(P .≥ -1e-14)

    return AR1Disc(xgrid, P)
end

struct XGrid; nodes::Vector{Float64}; end
build_xgrid_from_disc(d::AR1Disc) = XGrid(d.xgrid)

# ======================= Value Function with Stopping =====================

"""
solve_stopping_value_idioK (FAST, threaded, warm-start):
V(x;θ,κ_i,S) = max{ S, π̄(x;θ,κ_i) + β(1-δ)·E[V(x';θ,κ_i,S)] }
Returns V[Kx,Kθ,Kκ,KS].
- Accepts V_init (warm-start).
- Uses Howard every howard_every iterations for howard_iters evaluations.
- tol default 1e-5.
"""
function solve_stopping_value_idioK(
    X::XGrid, P::AbstractMatrix{<:Real}, params::Params,
    pbar::Float64, θgrid::Vector{Float64}, Kgrid::Vector{Float64}, Sgrid::Vector{Float64};
    κbar_unused::Float64=0.0,
    δ::Float64=0.0, λ::Float64=0.7, tol::Float64=1e-5, maxit::Int=2000,
    howard_every::Int=10, howard_iters::Int=20,
    stop_on_policy::Bool=false,
    V_init::Union{Nothing,Array{Float64,4}}=nothing,
    verbose::Bool=false
)
    Kx, Kθ, Kκ, KS = length(X.nodes), length(θgrid), length(Kgrid), length(Sgrid)
    @views Pm = Matrix{Float64}(P)

    # Flow profits Π[ix,iθ,ik]
    Π = Array{Float64,3}(undef, Kx, Kθ, Kκ)
    @inbounds for ix in 1:Kx
        base = pbar*ϕ(X.nodes[ix]) - c(X.nodes[ix], params)
        for iθ in 1:Kθ, ik in 1:Kκ
            Π[ix,iθ,ik] = base - θgrid[iθ]*Kgrid[ik]
        end
    end

    V  = isnothing(V_init) ? Array{Float64,4}(undef, Kx, Kθ, Kκ, KS) : copy(V_init)
    Vn = similar(V)
    policy = falses(size(V))
    policy_prev = falses(size(V))

    # initialize at S if no warm start
    if isnothing(V_init)
        @inbounds for iS in 1:KS
            V[:,:,:,iS] .= Sgrid[iS]
        end
    end

    stable_count = 0
    it = 0
    while it < maxit
        it += 1
        copyto!(policy_prev, policy)

        Threads.@threads for iθ in 1:Kθ
            EV = Vector{Float64}(undef, Kx)  # thread-local buffer
            for ik in 1:Kκ
                for iS in 1:KS
                    mul!(EV, Pm, @view V[:, iθ, ik, iS])
                    @inbounds @simd for ix in 1:Kx
                        cont = Π[ix,iθ,ik] + params.β*(1-δ)*EV[ix]
                        if cont >= Sgrid[iS]
                            Vn[ix,iθ,ik,iS] = cont
                            policy[ix,iθ,ik,iS] = true
                        else
                            Vn[ix,iθ,ik,iS] = Sgrid[iS]
                            policy[ix,iθ,ik,iS] = false
                        end
                    end
                end
            end
        end

        sup = maximum(abs.(Vn .- V))
        @. V = (1-λ)*V + λ*Vn
        verbose && @info "V_idioK iter $it: supnorm=$(round(sup,digits=6))"
        if sup < tol; break; end

        # early stop on policy stability
        if stop_on_policy && it > 5 && count(policy .⊻ policy_prev) == 0
            stable_count += 1
            if stable_count ≥ 2
                verbose && @info "Policy stable; stopping at iter $it"
                break
            end
        else
            stable_count = 0
        end

        if howard_every>0 && it % howard_every==0
            for _ in 1:howard_iters
                Threads.@threads for iθ in 1:Kθ
                    EV = Vector{Float64}(undef, Kx)
                    for ik in 1:Kκ, iS in 1:KS
                        mul!(EV, Pm, @view V[:, iθ, ik, iS])
                        @inbounds @simd for ix in 1:Kx
                            if policy[ix,iθ,ik,iS]
                                V[ix,iθ,ik,iS] = Π[ix,iθ,ik] + params.β*(1-δ)*EV[ix]
                            else
                                V[ix,iθ,ik,iS] = Sgrid[iS]
                            end
                        end
                    end
                end
            end
        end
    end
    return V
end

# =================== Stationary Equilibrium Components ====================
normalize_weights!(w) = (s = sum(w); s > 0 ? (w ./= s) : nothing)

function entrant_weights_on_grids_corr(
    Hθ::Distribution, Hκ::Distribution, HS::Distribution,
    X::XGrid, θgrid::Vector{Float64}, Kgrid::Vector{Float64}, Sgrid::Vector{Float64};
    N::Int=100_000, ρ::Float64=0.5, seed::Int=12345
)
    rng = MersenneTwister(seed)
    Kx, Kθ, Kκ, KS = length(X.nodes), length(θgrid), length(Kgrid), length(Sgrid)
    W4 = zeros(Float64, Kx, Kθ, Kκ, KS)
    μx = mean(X.nodes); σx = std(X.nodes)
    xdist = Normal(μx, σx>0 ? σx : 1.0)
    @inbounds for _ in 1:N
        x0 = rand(rng, xdist)
        θ  = rand(rng, Hθ)
        z1, z2 = randn(rng), randn(rng)
        z2 = ρ*z1 + sqrt(1-ρ^2)*z2
        uκ = cdf(Normal(), z1)
        uS = cdf(Normal(), z2)
        κi = quantile(Hκ, uκ)
        Si = quantile(HS, uS)
        ix = clamp(searchsortedfirst(X.nodes, x0), 1, Kx)
        iθ = clamp(searchsortedfirst(θgrid, θ), 1, Kθ)
        ik = clamp(searchsortedfirst(Kgrid, κi), 1, Kκ)
        iS = clamp(searchsortedfirst(Sgrid, Si), 1, KS)
        W4[ix,iθ,ik,iS] += 1.0
    end
    W4 ./= sum(W4)
    return W4
end

# --- helper: evaluate free-entry function at price p
function FE_residual(pval::Float64, prm::Params, X::XGrid, P::Matrix{Float64},
                     θg::Vector{Float64}, Kg::Vector{Float64}, Sg::Vector{Float64},
                     W4::Array{Float64,4}; F_e::Float64, δ::Float64,
                     λ::Float64=0.7, tol::Float64=1e-5, maxit::Int=2000,
                     howard_every::Int=10, howard_iters::Int=20,
                     V_init=nothing)
    Vbar, _, _, Vout = eval_at_price_idioK(
        pval, X, P, prm, θg, Kg, Sg, W4;
        δ=δ, λ=λ, tol=tol, maxit=maxit,
        howard_every=howard_every, howard_iters=howard_iters,
        V_init=V_init, verbose=false
    )
    return Vbar - F_e, Vout
end

# --- robust auto-bracketer on (ε, a-ε) with warm starts carried forward
function find_FE_bracket(prm::Params, X::XGrid, P::Matrix{Float64},
                         θg::Vector{Float64}, Kg::Vector{Float64}, Sg::Vector{Float64},
                         W4::Array{Float64,4};
                         F_e::Float64, δ::Float64,
                         pmin::Float64=1e-6, pmax::Float64=prevfloat(prm.a),
                         Ngrid::Int=24)
    @assert pmin > 0 && pmax < prm.a
    # scan from both ends; carry value-function warm start
    Vcache = nothing
    pgrid  = range(pmin, pmax; length=Ngrid)
    Fvals  = similar(collect(pgrid))
    for (i,pp) in enumerate(pgrid)
        Fp, Vcache = FE_residual(pp, prm, X, P, θg, Kg, Sg, W4;
                                 F_e=F_e, δ=δ, V_init=Vcache)
        Fvals[i] = Fp
    end
    # look for sign change
    for i in 1:length(pgrid)-1
        if Fvals[i] < 0.0 && Fvals[i+1] > 0.0
            return (pL=pgrid[i], pU=pgrid[i+1])
        end
    end
    # if no sign change, report diagnostics and throw
    @warn "No FE bracket on [$(pmin),$(pmax)]. "*
          "min F = $(minimum(Fvals)), max F = $(maximum(Fvals)). "*
          "Consider tuning Fe/ϕ/c or widen domain."
    error("FE bracket not found")
end


# --- deterministic W4 with Gaussian copula on (θ,S) instead of (κ,S) ---
# ---------- deterministic W4 with Gaussian copula on (κ,S) ----------
# helper: probability masses for a 1D grid using CDF bin widths
function bin_masses_from_cdf(grid::AbstractVector{<:Real}, F::Function; lo::Float64=0.0, hi::Float64=1.0)
    K = length(grid)
    @assert K ≥ 1
    u = clamp.(F.(grid), lo+1e-10, hi-1e-10)  # CDF locations of nodes
    Δ = similar(u)
    if K == 1
        Δ[1] = hi - lo
    else
        # midpoints in CDF space
        umid = similar(u, K-1)
        for i in 1:K-1
            umid[i] = 0.5*(u[i] + u[i+1])
        end
        # edges of the truncated region [lo, hi]
        Δ[1]     = max(umid[1] - lo, 0.0)
        Δ[K]     = max(hi - umid[K-1], 0.0)
        for i in 2:K-1
            Δ[i] = max(umid[i] - umid[i-1], 0.0)
        end
    end
    # renormalize to 1 within [lo,hi] (handles truncation)
    s = sum(Δ); s>0 ? (Δ ./= s) : nothing
    return (u=u, w=Δ)
end

# Gaussian copula density c_ρ(u,v) evaluated at CDF points (u,v)
@inline function gcopula_pdf(u::Float64, v::Float64, ρ::Float64)
    z1 = quantile(Normal(), u)
    z2 = quantile(Normal(), v)
    inv = 1.0 / sqrt(1 - ρ^2)
    expo = -0.5 * inv^2 * (z1^2 - 2ρ*z1*z2 + z2^2) + 0.5*(z1^2 + z2^2)
    # φ2(z; ρ) / (φ(z1)φ(z2)) simplifies to inv * exp(expo)
    return inv * exp(expo)
end

"""
entrant_weights_on_grids_corr_deterministic(Hθ, Hκ, HS, X, θgrid, Kgrid, Sgrid; ρ=0.5, xdist=:normal)
- Deterministic W4 on Kx×Kθ×Kκ×KS
- Marginals:
    * x: by default Normal(mean(X.nodes), std(X.nodes)) over the x grid via CDF bin widths
    * θ, κ, S: exact bin masses from their true CDFs at your grid nodes (handles truncated quantile grids)
- Dependence:
    * (κ,S) joint built with a Gaussian copula at correlation ρ using midpoint rectangle rule:
        WκS[i,k] ∝ c_ρ(uκ[i], uS[k]) * Δuκ[i] * ΔuS[k]
"""
function entrant_weights_on_grids_corr_deterministic(
    Hθ::Distribution, Hκ::Distribution, HS::Distribution,
    X::XGrid, θgrid::Vector{Float64}, Kgrid::Vector{Float64}, Sgrid::Vector{Float64};
    ρ::Float64=0.5, xdist::Symbol=:normal
)
    Kx, Kθ, Kκ, KS = length(X.nodes), length(θgrid), length(Kgrid), length(Sgrid)

    # (1) x marginal
    if xdist == :normal
        μx = mean(X.nodes); σx = std(X.nodes); σx = σx>0 ? σx : 1.0
        Fx = x -> cdf(Normal(μx, σx), x)
        _, wx = bin_masses_from_cdf(X.nodes, Fx; lo=0.0, hi=1.0)
    elseif xdist == :uniform
        wx = fill(1.0/Kx, Kx)
    else
        error("xdist must be :normal or :uniform")
    end

    # (2) θ marginal (handles truncated grids built from quantiles)
    Fθ = θ -> cdf(Hθ, θ)
    _, wθ = bin_masses_from_cdf(θgrid, Fθ; lo=0.0, hi=1.0)

    # (3) κ & S: get CDF locations and bin widths under their TRUE marginals
    Fκ = κ -> cdf(Hκ, κ)
    FS = s -> cdf(HS, s)
    uκ, Δuκ = bin_masses_from_cdf(Kgrid, Fκ)
    uS, ΔuS = bin_masses_from_cdf(Sgrid, FS)

    # (4) κ–S Gaussian copula joint via rectangle rule at node (uκ[i], uS[j])
    WκS = zeros(Float64, Kκ, KS)
    @inbounds for i in 1:Kκ, j in 1:KS
        WκS[i,j] = gcopula_pdf(uκ[i], uS[j], ρ) * Δuκ[i] * ΔuS[j]
    end
    # normalize (should already be close to 1 within truncation)
    WκS ./= sum(WκS)

    # (5) full W4 as product: w_x ⊗ w_θ ⊗ W_{κ,S}
    W4 = zeros(Float64, Kx, Kθ, Kκ, KS)
    @inbounds for ix in 1:Kx
        for iθ in 1:Kθ
            for iκ in 1:Kκ
                for iS in 1:KS
                    W4[ix,iθ,iκ,iS] = wx[ix]*wθ[iθ]*WκS[iκ,iS]
                end
            end
        end
    end
    # final normalization guard
    W4 ./= sum(W4)
    return W4
end

function entrant_weights_on_grids_thetaS_corr_deterministic(
    Hθ::Distribution, Hκ::Distribution, HS::Distribution,
    X::XGrid, θgrid::Vector{Float64}, Kgrid::Vector{Float64}, Sgrid::Vector{Float64};
    ρ::Float64=0.5, xdist::Symbol=:normal
)
    Kx, Kθ, Kκ, KS = length(X.nodes), length(θgrid), length(Kgrid), length(Sgrid)

    # x marginal (same as before)
    if xdist == :normal
        μx = mean(X.nodes); σx = std(X.nodes); σx = σx>0 ? σx : 1.0
        Fx = x -> cdf(Normal(μx, σx), x)
        _, wx = bin_masses_from_cdf(X.nodes, Fx; lo=0.0, hi=1.0)
    elseif xdist == :uniform
        wx = fill(1.0/Kx, Kx)
    else
        error("xdist must be :normal or :uniform")
    end

    # κ marginal (independent)
    Fκ = κ -> cdf(Hκ, κ)
    _, wκ = bin_masses_from_cdf(Kgrid, Fκ)

    # θ and S: correlate via Gaussian copula in CDF space
    Fθ = θ -> cdf(Hθ, θ)
    FS = s -> cdf(HS, s)
    uθ, Δuθ = bin_masses_from_cdf(θgrid, Fθ)     # CDF locations + bin widths
    uS, ΔuS = bin_masses_from_cdf(Sgrid, FS)

    # Build joint for (θ,S) by rectangle rule at (uθ[i], uS[j])
    WθS = zeros(Float64, Kθ, KS)
    @inbounds for i in 1:Kθ, j in 1:KS
        WθS[i,j] = gcopula_pdf(uθ[i], uS[j], ρ) * Δuθ[i] * ΔuS[j]
    end
    WθS ./= sum(WθS)  # normalize

    # Assemble W4 = w_x ⊗ w_κ ⊗ W_{θ,S}
    W4 = zeros(Float64, Kx, Kθ, Kκ, KS)
    @inbounds for ix in 1:Kx
        for ik in 1:Kκ
            for iθ in 1:Kθ, iS in 1:KS
                W4[ix,iθ,ik,iS] = wx[ix] * wκ[ik] * WθS[iθ,iS]
            end
        end
    end
    W4 ./= sum(W4)
    return W4
end

"""
FAST eval_at_price_idioK:
- Warm-start via V_init (passed in/out).
- Skip zero-mass blocks when building μ and q̄.
Returns (V̄_ent, q̄, exit_per_unit, V_out).
"""
function eval_at_price_idioK(
    p_now::Float64, X::XGrid, P::Matrix{Float64}, params::Params,
    θgrid::Vector{Float64}, Kgrid::Vector{Float64}, Sgrid::Vector{Float64},
    W4::Array{Float64,4}; δ::Float64=0.0, λ::Float64=0.7, tol::Float64=1e-5,
    maxit::Int=2000, howard_every::Int=10, howard_iters::Int=20,
    V_init::Union{Nothing,Array{Float64,4}}=nothing, verbose::Bool=false
)
    Kx, Kθ, Kκ, KS = length(X.nodes), length(θgrid), length(Kgrid), length(Sgrid)

    # Solve V at p_now (warm-start from V_init if provided)
    V = solve_stopping_value_idioK(
        X, P, params, p_now, θgrid, Kgrid, Sgrid;
        δ=δ, λ=λ, tol=tol, maxit=maxit,
        howard_every=howard_every, howard_iters=howard_iters,
        stop_on_policy=false,
        V_init=V_init, verbose=verbose
    )

    # Expected entrant value
    Vbar_ent = sum(W4 .* V)

    # Build continuation mask once
    Π = Array{Float64,3}(undef, Kx, Kθ, Kκ)
    @inbounds for ix in 1:Kx
        base = p_now*ϕ(X.nodes[ix]) - c(X.nodes[ix], params)

        for iθ in 1:Kθ, ik in 1:Kκ
            Π[ix,iθ,ik] = base - θgrid[iθ]*Kgrid[ik]
        end
    end

    EV = similar(V)
    @inbounds for iθ in 1:Kθ, ik in 1:Kκ, iS in 1:KS
        EV[:,iθ,ik,iS] = P * view(V, :, iθ, ik, iS)
    end

    C = falses(size(V))
    @inbounds for ix in 1:Kx, iθ in 1:Kθ, ik in 1:Kκ, iS in 1:KS
        C[ix,iθ,ik,iS] = Π[ix,iθ,ik] + params.β*(1-δ)*EV[ix,iθ,ik,iS] >= Sgrid[iS]
    end

    # Precompute active blocks (skip zero-entry mass)
    blockmass = sum(W4; dims=1)              # 1×Kθ×Kκ×KS
    active = Tuple{Int,Int,Int}[]
    for iθ in 1:Kθ, ik in 1:Kκ, iS in 1:KS
        if blockmass[1,iθ,ik,iS] > 1e-14
            push!(active, (iθ,ik,iS))
        end
    end

    yvec = y.(X.nodes)
    qbar = 0.0
    exit_per_unit = 0.0

    # Lifetime per active block
    for (iθ,ik,iS) in active
        wblock = view(W4, :, iθ, ik, iS)
        Ablk = copy(P)
        @inbounds for ix in 1:Kx
            if C[ix,iθ,ik,iS]
                Ablk[ix,:] .*= (1 - δ)
            else
                Ablk[ix,:] .= 0.0
            end
        end
        F = lu!(I - Ablk)
        μx = F \ wblock
        qbar += dot(yvec, μx)
        @inbounds for ix in 1:Kx
            exit_per_unit += μx[ix] * (C[ix,iθ,ik,iS] ? δ : 1.0)
        end
    end

    return (Vbar_ent, qbar, exit_per_unit, V)
end

"""
solve_stationary_equilibrium_idioK (FAST):
- Bisection with warm-started V across price evaluations.
- tol_fe default 1e-4.
- Generic demand inversion Q_from_p.
"""
function solve_stationary_equilibrium_idioK(
    params::Params, X::XGrid, P::Matrix{Float64},
    θgrid::Vector{Float64}, Kgrid::Vector{Float64}, Sgrid::Vector{Float64},
    W4::Array{Float64,4};
    Q_from_p::Function, p_lo::Float64, p_hi::Float64,
    F_e::Float64, δ::Float64=0.0,
    λ::Float64=0.7, tol_fe::Float64=1e-4, maxit_fe::Int=60,
    tol_fe_val::Float64=1e-3,   # <-- NEW: tolerance on F(p)
    verbose::Bool=false
)
    # warm-start cache (must be mutable to update from closure)
    Vcache = Ref{Union{Nothing,Array{Float64,4}}}(nothing)

    F_of_p = function (pval::Float64)
        Vbar, _, _, Vout = eval_at_price_idioK(
            pval, X, P, params, θgrid, Kgrid, Sgrid, W4;
            δ=δ, λ=λ, V_init=Vcache[], verbose=false,
        )
        Vcache[] = Vout
        return Vbar - F_e
    end

    Flo = F_of_p(p_lo)
    Fhi = F_of_p(p_hi)
    
    if !(Flo < 0 && Fhi > 0)
        @info "User bracket failed (Flo=$(Flo), Fhi=$(Fhi)). Attempting auto-bracket…"
        br = find_FE_bracket(params, X, P, θgrid, Kgrid, Sgrid, W4;
                             F_e=F_e, δ=δ, pmin=1e-6, pmax=prevfloat(params.a), Ngrid=24)
        p_lo, p_hi = br.pL, br.pU
        Flo, Fhi = F_of_p(p_lo), F_of_p(p_hi)
        @info "Auto-bracket found: p_lo=$(p_lo) (F=$(Flo)), p_hi=$(p_hi) (F=$(Fhi))"
    end
    @assert Flo < 0 && Fhi > 0 "Bracket must satisfy F(p_lo)<0<F(p_hi)."
    pL, pU, FL, FU = p_lo, p_hi, Flo, Fhi
    for _ in 1:maxit_fe
        pm = 0.5*(pL + pU)
        Fm = F_of_p(pm)
        # primary criterion: function value small
        if abs(Fm) < tol_fe_val
            pL = pU = pm
            break
        end
        # secondary: tiny bracket (only if also reasonably small value)
        if (pU - pL) < tol_fe && abs(Fm) < 10*tol_fe_val
            pL = pU = pm
            break
        end
        if Fm > 0.0
            pU, FU = pm, Fm
        else
            pL, FL = pm, Fm
        end
    end
    p_star = 0.5*(pL + pU)

    Vbar_star, qbar_star, exit_unit, _ = eval_at_price_idioK(
        p_star, X, P, params, θgrid, Kgrid, Sgrid, W4;
        δ=δ, λ=λ, V_init=Vcache[], verbose=verbose
    )

    # market clearing
    Q_star = max(0.0, Q_from_p(p_star))
    m_star = Q_star / max(qbar_star, 1e-12)

    if verbose
        F_check = Vbar_star - F_e
        @info "Free-entry check at p*=$(round(p_star,digits=6)): F(p*)=$(round(F_check,digits=6))"
    end
    return (p=p_star, m=m_star, Q=Q_star, qbar=qbar_star, Vbar=Vbar_star, exit_per_unit=exit_unit)
end

# ---- Weighted utilities (unchanged) --------------------------------------
function w_mean_std(z::AbstractVector{<:Real}, w::AbstractVector{<:Real})
    wz = sum(@. w*z)
    wz2 = sum(@. w*z*z)
    var = max(wz2 - wz^2, 0.0)
    return (μ=wz, σ=sqrt(var))
end

function w_quantiles(z::AbstractVector{<:Real}, w::AbstractVector{<:Real}, ps::Vector{Float64})
    @assert length(z) == length(w)
    idx = sortperm(z)
    zs = z[idx]; ws = copy(w[idx]); normalize_weights!(ws)
    cdf = cumsum(ws)
    out = similar(ps)
    for (j,p) in enumerate(ps)
        k = searchsortedfirst(cdf, p)
        if k <= 1
            out[j] = zs[1]
        elseif k > length(zs)
            out[j] = zs[end]
        else
            out[j] = zs[k]
        end
    end
    return out
end

function w_cov_corr(U::AbstractVector, V::AbstractVector, W::AbstractMatrix)
    Wn = copy(W); normalize_weights!(Wn)
    μu = sum(sum(Wn, dims=2)[:].*U)
    μv = sum(sum(Wn, dims=1)[:].*V)
    Eu2 = sum(sum(Wn, dims=2)[:].*(U.^2))
    Ev2 = sum(sum(Wn, dims=1)[:].*(V.^2))
    cross = 0.0
    for iu in eachindex(U), iv in eachindex(V)
        cross += Wn[iu,iv]*U[iu]*V[iv]
    end
    cov = max(cross - μu*μv, 0.0)
    σu, σv = sqrt(max(Eu2 - μu^2, 0.0)), sqrt(max(Ev2 - μv^2, 0.0))
    ρ = (σu>0 && σv>0) ? cov/(σu*σv) : 0.0
    return (μu=μu, μv=μv, σu=σu, σv=σv, cov=cov, ρ=ρ)
end

function entrant_diagnostics(W4::Array{Float64,4},
                             Xnodes::Vector{Float64}, θg::Vector{Float64},
                             Kg::Vector{Float64}, Sg::Vector{Float64})
    W = copy(W4); normalize_weights!(W)
    Kx, Kθ, Kκ, KS = size(W)
    WθκS = dropdims(sum(W, dims=1), dims=1)
    Wθ   = sum(sum(WθκS, dims=3), dims=2)[:]
    Wκ   = sum(sum(WθκS, dims=3), dims=1)[:]
    WS   = sum(sum(WθκS, dims=2), dims=1)[:]
    ϕvals = [θg[iθ]*Kg[ik] for iθ in 1:Kθ, ik in 1:Kκ]
    Wϕ    = sum(WθκS, dims=3)[:, :, 1]
    Wϕv   = vec(Wϕ); ϕv = vec(ϕvals); normalize_weights!(Wϕv)
    mθ = w_mean_std(θg, Wθ); mκ = w_mean_std(Kg, Wκ); mS = w_mean_std(Sg, WS); mϕ = w_mean_std(ϕv, Wϕv)
    ps = [0.10, 0.25, 0.50, 0.75, 0.90]
    qθ = w_quantiles(θg, Wθ, ps); qκ = w_quantiles(Kg, Wκ, ps); qS = w_quantiles(Sg, WS, ps); qϕ = w_quantiles(ϕv, Wϕv, ps)
    Wθκ = sum(WθκS, dims=3)[:, :, 1]
    cθκ = w_cov_corr(θg, Kg, Wθκ)
    WκS = permutedims(sum(WθκS, dims=1)[1, :, :], (1,2))
    cκS = w_cov_corr(Kg, Sg, WκS)
    WθS = sum(WθκS, dims=2)[:, 1, :]
    cθS = w_cov_corr(θg, Sg, WθS)
    WϕS = zeros(length(Wϕv), length(Sg))
    idx = 0
    for iθ in 1:Kθ, ik in 1:Kκ
        idx += 1
        for iS in 1:KS
            WϕS[idx,iS] = sum(WθκS[iθ,ik,iS])
        end
    end
    cϕS = w_cov_corr(ϕv, Sg, WϕS)
    S90 = qS[end]; κ90 = qκ[end]
    share_highS = sum(WS[iS] for iS in 1:KS if Sg[iS] >= S90)
    share_highκ = sum(Wκ[ik] for ik in 1:Kκ if Kg[ik] >= κ90)

    return (
        entrants = (
            mean = (θ=mθ.μ, κ=mκ.μ, S=mS.μ, ϕ=mϕ.μ),
            sd   = (θ=mθ.σ, κ=mκ.σ, S=mS.σ, ϕ=mϕ.σ),
            q10_25_50_75_90 = (θ=qθ, κ=qκ, S=qS, ϕ=qϕ),
            corr = (θκ=cθκ.ρ, κS=cκS.ρ, θS=cθS.ρ, ϕS=cϕS.ρ),
            top_decile_share = (highS=share_highS, highκ=share_highκ)
        )
    )
end

function incumbent_diagnostics(
    p_now::Float64, params::Params, X::XGrid, P::Matrix{Float64},
    θg::Vector{Float64}, Kg::Vector{Float64}, Sg::Vector{Float64},
    W4::Array{Float64,4}; δ::Float64=0.0, λ::Float64=0.7, tol::Float64=1e-5,
    maxit::Int=2000, howard_every::Int=10, howard_iters::Int=20,
    scale_m::Float64=1.0
)
    Kx, Kθ, Kκ, KS = length(X.nodes), length(θg), length(Kg), length(Sg)

    # (i) Solve V and continuation at p_now
    V = solve_stopping_value_idioK(X, P, params, p_now, θg, Kg, Sg;
                                   δ=δ, λ=λ, tol=tol, maxit=maxit,stop_on_policy=false,
                                   howard_every=howard_every, howard_iters=howard_iters)

    Π = Array{Float64,3}(undef, Kx, Kθ, Kκ)
    @inbounds for ix in 1:Kx
        base = p_now*ϕ(X.nodes[ix]) - c(X.nodes[ix], params)

        for iθ in 1:Kθ, ik in 1:Kκ
            Π[ix,iθ,ik] = base - θg[iθ]*Kg[ik]
        end
    end
    EV = similar(V)
    @inbounds for iθ in 1:Kθ, ik in 1:Kκ, iS in 1:KS
        EV[:,iθ,ik,iS] = P * view(V, :, iθ, ik, iS)
    end
    C = falses(size(V))
    @inbounds for ix in 1:Kx, iθ in 1:Kθ, ik in 1:Kκ, iS in 1:KS
        C[ix,iθ,ik,iS] = Π[ix,iθ,ik] + params.β*(1-δ)*EV[ix,iθ,ik,iS] >= Sg[iS]
    end

    # (ii) Active blocks (skip zero entrant mass)
    blockmass = sum(W4; dims=1)              # 1×Kθ×Kκ×KS
    active = Tuple{Int,Int,Int}[]
    for iθ in 1:Kθ, ik in 1:Kκ, iS in 1:KS
        if blockmass[1,iθ,ik,iS] > 1e-14
            push!(active, (iθ,ik,iS))
        end
    end

    # (iii) Build per-unit entry stationary stock and exit composition
    yvec = y.(X.nodes)
    qbar = 0.0
    stop_now_prob = 0.0
    hazard_prob   = 0.0

    μ_unit = zeros(Float64, Kx, Kθ, Kκ, KS)

    for (iθ,ik,iS) in active
        wblock = view(W4, :, iθ, ik, iS)

        # immediate exits at t=0 (those starting in stop states)
        stop_now_prob += sum((.~C[:,iθ,ik,iS]) .* wblock)

        # continuation operator
        Ablk = copy(P)
        @inbounds for ix in 1:Kx
            if C[ix,iθ,ik,iS]
                Ablk[ix,:] .*= (1 - δ)   # continue & survive
            else
                Ablk[ix,:] .= 0.0        # absorb at stop
            end
        end
        μx = (I - Ablk) \ wblock
        μ_unit[:,iθ,ik,iS] = μx

        qbar += dot(yvec, μx)
        hazard_prob += δ * sum(μx .* C[:,iθ,ik,iS])
    end

    exit_per_unit = stop_now_prob + hazard_prob  # ≈ 1.0

    # (iv) Collapse x to get type distribution among incumbents (scaled if desired)
    μ_types = dropdims(sum(μ_unit, dims=1), dims=1) * scale_m # Kθ×Kκ×KS
    total_mass = sum(μ_types) + 1e-16
    Wθ = sum(sum(μ_types, dims=3), dims=2)[:] ./ total_mass
    Wκ = sum(sum(μ_types, dims=3), dims=1)[:] ./ total_mass
    WS = sum(sum(μ_types, dims=2), dims=1)[:] ./ total_mass

    ϕv  = vec([θg[iθ]*Kg[ik] for iθ in 1:Kθ, ik in 1:Kκ])
    Wϕv = vec(sum(μ_types, dims=3)) ./ total_mass

    mθ = w_mean_std(θg, Wθ); mκ = w_mean_std(Kg, Wκ); mS = w_mean_std(Sg, WS)
    ps = [0.10, 0.25, 0.50, 0.75, 0.90]
    qθ = w_quantiles(θg, Wθ, ps); qκ = w_quantiles(Kg, Wκ, ps); qS = w_quantiles(Sg, WS, ps)
    mϕ = w_mean_std(ϕv, Wϕv);     qϕ = w_quantiles(ϕv, Wϕv, ps)

    Wθκ = sum(μ_types, dims=3)[:, :, 1]; normalize_weights!(Wθκ)
    WκS = permutedims(sum(μ_types, dims=1)[1, :, :], (1,2)); normalize_weights!(WκS)
    WθS = sum(μ_types, dims=2)[:, 1, :]; normalize_weights!(WθS)
    cθκ = w_cov_corr(θg, Kg, Wθκ)
    cκS = w_cov_corr(Kg, Sg, WκS)
    cθS = w_cov_corr(θg, Sg, WθS)

    # ϕ–S correlation (empirical support)
    WϕS = zeros(length(Wϕv), length(Sg))
    idx = 0
    for iθ in 1:Kθ, ik in 1:Kκ
        idx += 1
        for iS in 1:KS
            WϕS[idx,iS] = μ_types[iθ,ik,iS]
        end
    end
    normalize_weights!(WϕS)
    cϕS = w_cov_corr(ϕv, Sg, WϕS)

    return (
        per_unit = (qbar=qbar, exit_per_unit=exit_per_unit,
                    stop_now=stop_now_prob, hazard=hazard_prob),
        totals   = (mass=total_mass, qbar_scaled=qbar*scale_m),
        incumbents = (
            mean = (θ=mθ.μ, κ=mκ.μ, S=mS.μ, ϕ=mϕ.μ),
            sd   = (θ=mθ.σ, κ=mκ.σ, S=mS.σ, ϕ=mϕ.σ),
            q10_25_50_75_90 = (θ=qθ, κ=qκ, S=qS, ϕ=qϕ),
            corr = (θκ=cθκ.ρ, κS=cκS.ρ, θS=cθS.ρ, ϕS=cϕS.ρ)
        )
    )
end

using DataFrames, CSV, StatsBase

# === ϕ(x) used in profits (same as in your model) ===

"""
export_distributions(p_now, params, X, P, θg, Kg, Sg, W4; δ, scale_m, prefix)
- Writes two CSVs:
- scale_m = m* if you want actual levels; 1.0 gives per-unit-entry masses.
"""
function export_distributions(
    p_now::Float64, params::Params, X::XGrid, P::Matrix{Float64},
    θg::Vector{Float64}, Kg::Vector{Float64}, Sg::Vector{Float64},
    W4::Array{Float64,4};
    δ::Float64=0.0, λ::Float64=0.7, tol::Float64=1e-5, maxit::Int=2000,
    howard_every::Int=10, howard_iters::Int=20,
    scale_m::Float64=1.0, prefix::AbstractString="se"
)
    Kx, Kθ, Kκ, KS = length(X.nodes), length(θg), length(Kg), length(Sg)

    # ---------- Entrant distribution over x ----------
    w_x = vec(sum(W4; dims=(2,3,4)))               # Kx
    wx_norm = w_x ./ max(sum(w_x), 1e-16)
    df_ent = DataFrame(x = X.nodes,
                       weight = wx_norm)
    #CSV.write("$(prefix)_entrants_x.csv", df_ent)

    # ---------- Incumbent distribution over x ----------
    # Solve V and continuation, then lifetime stock per unit entry
    V = solve_stopping_value_idioK(X, P, params, p_now, θg, Kg, Sg;
                                   δ=δ, λ=λ, tol=tol, maxit=maxit,
                                   howard_every=howard_every, howard_iters=howard_iters)

    # Flow profit pieces to build continuation set
    Π = Array{Float64,3}(undef, Kx, Kθ, Kκ)
    @inbounds for ix in 1:Kx
        base = p_now*ϕ(X.nodes[ix]) - c(X.nodes[ix], params)
        for iθ in 1:Kθ, ik in 1:Kκ
            Π[ix,iθ,ik] = base - θg[iθ]*Kg[ik]
        end
    end
    EV = similar(V)
    @inbounds for iθ in 1:Kθ, ik in 1:Kκ, iS in 1:KS
        EV[:,iθ,ik,iS] = P * view(V, :, iθ, ik, iS)
    end
    C = falses(size(V))
    @inbounds for ix in 1:Kx, iθ in 1:Kθ, ik in 1:Kκ, iS in 1:KS
        C[ix,iθ,ik,iS] = Π[ix,iθ,ik] + params.β*(1-δ)*EV[ix,iθ,ik,iS] >= Sg[iS]
    end

    # Per-unit-entry lifetime stock μ over x (sum over types)
    μx_total = zeros(Float64, Kx)
    blockmass = sum(W4; dims=1)  # 1×Kθ×Kκ×KS
    for iθ in 1:Kθ, ik in 1:Kκ, iS in 1:KS
        if blockmass[1,iθ,ik,iS] ≤ 1e-14; continue; end
        wblock = view(W4, :, iθ, ik, iS)
        Ablk = copy(P)
        @inbounds for ix in 1:Kx
            if C[ix,iθ,ik,iS]; Ablk[ix,:] .*= (1-δ) else; Ablk[ix,:] .= 0.0 end
        end
        μx = (I - Ablk) \ wblock
        μx_total .+= μx
    end

    # Scale by m* if desired (scale_m = 1.0 gives per-unit-entry)
    μx_scaled = μx_total .* scale_m
    mass_total = sum(μx_scaled)
    share_x = μx_scaled ./ max(mass_total, 1e-16)

    # Effective price for each x is p*ϕ(x)
    eff_price = p_now .* ϕ.(X.nodes)

    df_inc = DataFrame(x = X.nodes,
                       mass = μx_scaled,
                       share = share_x,
                       eff_price = eff_price)
    #CSV.write("$(prefix)_incumbents_x.csv", df_inc)

    # (Optional) quick histogram CSV for eff_price among incumbents
    h = fit(Histogram, eff_price, weights(μx_scaled))
    df_hist = DataFrame(p_left = h.edges[1][1:end-1],
                        p_right = h.edges[1][2:end],
                        mass = h.weights)
    #CSV.write("$(prefix)_incumbents_effprice_hist.csv", df_hist)

    return (entrants=df_ent, incumbents=df_inc, effprice_hist=df_hist)
end


# returns incumbent weights over x at price p_now (per unit entry)
function incumbent_x_weights(p_now, prm, X, P, θg, Kg, Sg, W4; δ=0.001)
    V = solve_stopping_value_idioK(X, P, prm, p_now, θg, Kg, Sg; δ=δ, λ=0.7,
                                   howard_every=10, howard_iters=20, tol=1e-5, maxit=2000)
    # continuation mask
    Kx, Kθ, Kκ, KS = length(X.nodes), length(θg), length(Kg), length(Sg)
    Π = Array{Float64,3}(undef, Kx, Kθ, Kκ)
    for ix in 1:Kx
        base = p_now*y(X.nodes[ix]) - c(X.nodes[ix], prm)
        for iθ in 1:Kθ, ik in 1:Kκ
            Π[ix,iθ,ik] = base - θg[iθ]*Kg[ik]
        end
    end
    EV = similar(V)
    for iθ in 1:Kθ, ik in 1:Kκ, iS in 1:KS
        EV[:,iθ,ik,iS] = P * view(V, :, iθ, ik, iS)
    end
    C = falses(size(V))
    for ix in 1:Kx, iθ in 1:Kθ, ik in 1:Kκ, iS in 1:KS
        C[ix,iθ,ik,iS] = Π[ix,iθ,ik] + prm.β*(1-δ)*EV[ix,iθ,ik,iS] >= Sg[iS]
    end
    # per-block lifetime stock in x, summed over (θ,κ,S)
    Wx = zeros(Kx)
    blockmass = sum(W4; dims=1)
    for iθ in 1:Kθ, ik in 1:Kκ, iS in 1:KS
        if blockmass[1,iθ,ik,iS] ≤ 1e-14; continue; end
        wblock = view(W4, :, iθ, ik, iS)
        Ablk = copy(P)
        for ix in 1:Kx
            if C[ix,iθ,ik,iS]; Ablk[ix,:] .*= (1 - δ) else Ablk[ix,:] .= 0.0 end
        end
        μx = (I - Ablk) \ wblock
        Wx .+= μx
    end
    Wx ./= sum(Wx)
    return Wx
end

# one-step gamma rescale to hit a target sd of log prices
function retune_gamma!(γ::Base.RefValue{Float64}, pstar, prm, X, P, θg, Kg, Sg, W4;
                       δ=0.001, target_sd_logp::Float64)
    Wx_inc = incumbent_x_weights(pstar, prm, X, P, θg, Kg, Sg, W4; δ=δ)
    x = X.nodes
    μx = sum(Wx_inc .* x)
    σx2 = sum(Wx_inc .* (x .- μx).^2)
    # current sd(log p_eff) with current gamma
    ϕx = exp.(γ[]*(x .- μx) .- 0.5*γ[]^2*σx2)
    logp = log.(pstar .* ϕx)
    μlp = sum(Wx_inc .* logp)
    sdlp = sqrt(sum(Wx_inc .* (logp .- μlp).^2))
    # rescale gamma
    γ[] *= target_sd_logp / max(sdlp, 1e-12)
    return sdlp
end



# =============================== Example Run ==============================

# --- primitives ---
S_mean = 100000.0 / 12.0
Hθ = LogNormal(log(0.01), 2)
Hκ = LogNormal(log(2400.0), 0.4)
HS = LogNormal(log(S_mean), 0.6)

# Params instance (use `prm`, not `params`, to avoid name collision with Distributions.params)
prm = Params(
    0.95, # beta
    3500.0, # intercept a
    -0.003, # slope b
    600,   # c̄
    0.99,  # ρx
    0.25)  # σx

# check prices at reasonable Q


# Discretize x
Kx = 241           # slightly smaller default for speed
disc_x = rouwenhorst(Kx, prm.ρx, prm.σx; μ=0.0)
X = build_xgrid_from_disc(disc_x)
P = disc_x.P
μx  = mean(X.nodes)
σx2 = var(X.nodes)
const γ = 0.05 # dispersion knob; try 0.05–0.20

ϕ(x) = exp(γ*(x - μx) - 0.5*γ^2*σx2)  
#check that X -> sensible prices
p_check = prm.a * exp(prm.b * (1100.0))

# Heterogeneity grids (quantiles)
Kθ = 17; Kκ = 1; KS = 17       # start with no κ shock: Kκ=1, Kg=[0.0]
θg = collect(quantile(Hθ, range(0.05, 0.95; length=Kθ)))
Kg = [0.0]                     # no kappa shock counterfactual
Sg = collect(quantile(HS, range(0.05, 0.95; length=KS)))

# Entrant weights (κ–S correlation irrelevant when Kκ=1)
W4 = entrant_weights_on_grids_thetaS_corr_deterministic(
    Hθ, Hκ, HS, X, θg, Kg, Sg; ρ=-0.5
)

# Demand inversion helper: log-linear p = a*exp(bQ), b<0
@assert prm.a > 0
@assert prm.b < 0
Q_from_p(pp::Float64) = log(pp / prm.a) / prm.b
p_lo, p_hi = 1e-6, prevfloat(prm.a)

# hazard & FE tolerance
δ = 0.001
Fe = 150000.0 / 12.0

# Solve SE (FAST)
sol_idiok = solve_stationary_equilibrium_idioK(
    prm, X, P, θg, Kg, Sg, W4;
    Q_from_p=Q_from_p, p_lo=p_lo, p_hi=p_hi, F_e=Fe,
    δ=δ, λ=0.7, tol_fe=1e-4, maxit_fe=50, verbose=false
)

Vbar_chk, qbar_chk, _, _ = eval_at_price_idioK(sol_idiok.p, X, P, prm, θg, Kg, Sg, W4;
                                               δ=δ, λ=0.7, V_init=nothing,  # fresh eval
                                               howard_every=10, howard_iters=20,
                                               # crucially:
                                               )
println("Free-entry residual F(p*): ", Vbar_chk - Fe)

# Diagnostics
p_diag = sol_idiok.p
m_star = sol_idiok.m

Ediag = entrant_diagnostics(W4, X.nodes, θg, Kg, Sg)
println("Entrants mean θ, κ, S, ϕ: ", Ediag.mean)
println("Entrants corr θκ, κS, θS, ϕS: ", Ediag.corr)

Idiag_unit = incumbent_diagnostics(p_diag, prm, X, P, θg, Kg, Sg, W4; δ=δ, scale_m=1.0)
Idiag      = incumbent_diagnostics(p_diag, prm, X, P, θg, Kg, Sg, W4; δ=δ, scale_m=m_star)

println("=== Stationary equilibrium (no κ shock) ===")
println("p*  = ", round(sol_idiok.p,   digits=6))
println("Q*  = ", round(sol_idiok.Q,   digits=6))
println("m*  = ", round(sol_idiok.m,   digits=6))
println("q̄   = ", round(sol_idiok.qbar, digits=6))
println("E[V]_ent(p*) = ", round(sol_idiok.Vbar, digits=6), "  (target F_e = ", Fe, ")")
println("exit_per_unit = ", round(sol_idiok.exit_per_unit, digits=6))
println("Check: m*·q̄ = ", sol_idiok.m * sol_idiok.qbar, "  vs Q* = ", sol_idiok.Q)

p_star = sol_idiok.p
m_star = sol_idiok.m

price_summary_stats = export_distributions(p_star, prm, X, P, θg, Kg, Sg, W4;
                     δ=δ, scale_m=m_star, prefix="se_nokappa")

# plot the histogram of effective prices
using StatsBase
using Plots
histogram(price_summary_stats.effprice_hist.p_left,
          price_summary_stats.effprice_hist.p_right,
          weights=price_summary_stats.effprice_hist.mass,
          nbins=10, xlabel="Effective price", ylabel="Mass",
          title="Incumbent effective price distribution (no κ shock)")

histogram(price_summary_stats.incumbents.eff_price,
          weights=price_summary_stats.incumbents.mass,
          nbins=30, xlabel="Effective price", ylabel="Mass",
          title="Incumbent effective price distribution (no κ shock)"
          )

# weighted quantiles of effective prices
q_effp = w_quantiles(price_summary_stats.incumbents.eff_price,
                     price_summary_stats.incumbents.mass,
                     [0.10, 0.25, 0.50, 0.75, 0.90])
println("Effective price quantiles (no κ shock): ", q_effp)

# describe price_summary_stats
# --------- κ shock counterfactual ------------
Kκ = 17
k_mean, k_sd = 2000, 4
Hκ = LogNormal(log(k_mean), log(k_sd))
Kg = collect(quantile(Hκ, range(0.05, 0.95; length=Kκ)))
W4_pos = entrant_weights_on_grids_thetaS_corr_deterministic(
    Hθ, Hκ, HS, X, θg, Kg, Sg; ρ=0.5
)

sol_idiok_kshock_pos = solve_stationary_equilibrium_idioK(
    prm, X, P, θg, Kg, Sg, W4_pos   ;
    Q_from_p=Q_from_p, p_lo=p_lo, p_hi=p_hi, F_e=Fe,
    δ=δ, λ=0.7, tol_fe=1e-4, maxit_fe=50
)

# repeat but make the correlation negative between κ and S
W4_neg = entrant_weights_on_grids_thetaS_corr_deterministic(
    Hθ, Hκ, HS, X, θg, Kg, Sg; ρ=-0.5
)

sol_idiok_kshock_neg = solve_stationary_equilibrium_idioK(
    prm, X, P, θg, Kg, Sg, W4_neg;
    Q_from_p=Q_from_p, p_lo=p_lo, p_hi=p_hi, F_e=Fe,
    δ=δ, λ=0.7, tol_fe=1e-4, maxit_fe=50
)

Ediag_kshock_pos = entrant_diagnostics(W4_pos, X.nodes, θg, Kg, Sg)
Idiag_unit_kshock_pos = incumbent_diagnostics(sol_idiok_kshock_pos.p, prm, X, P, θg, Kg, Sg, W4_pos; δ=δ, scale_m=1.0)
Idiag_unit_kshock_pos      = incumbent_diagnostics(sol_idiok_kshock_pos.p, prm, X, P, θg, Kg, Sg, W4_pos; δ=δ, scale_m=sol_idiok_kshock_pos.m)


Ediag_kshock_neg = entrant_diagnostics(W4_neg, X.nodes, θg, Kg, Sg)
Idiag_unit_kshock_neg = incumbent_diagnostics(sol_idiok_kshock_neg.p, prm, X, P, θg, Kg, Sg, W4_neg; δ=δ, scale_m=1.0)
Idiag_kshock_neg      = incumbent_diagnostics(sol_idiok_kshock_neg.p, prm, X, P, θg, Kg, Sg, W4_neg; δ=δ, scale_m=sol_idiok_kshock_neg.m)


println("\n=== Δ (κ shock neg corr − κ shock pos corr) ===")
println("Δp*  = ", round(sol_idiok_kshock_neg.p - sol_idiok_kshock_pos.p, digits=6))
println("ΔQ*  = ", round(sol_idiok_kshock_neg.Q - sol_idiok_kshock_pos.Q, digits=6))
println("Δm*  = ", round(sol_idiok_kshock_neg.m - sol_idiok_kshock_pos.m, digits=6))
println("Δq̄  = ",       round(sol_idiok_kshock_neg.qbar - sol_idiok_kshock_pos.qbar, digits=6))
println("ΔE[V]_ent = ", round(sol_idiok_kshock_neg.Vbar - sol_idiok_kshock_pos.Vbar, digits=6))
println("Δexit/unit = ", round(sol_idiok_kshock_neg.exit_per_unit - sol_idiok_kshock_pos.exit_per_unit, digits=6))
# (end of file)

# quantiles of effective prices
price_summary_stats_kshock_pos = export_distributions(sol_idiok_kshock_pos.p, prm, X, P, θg, Kg, Sg, W4_pos;
                     δ=δ, scale_m=sol_idiok_kshock_pos.m, prefix="se_kappa_pos")

price_summary_stats_kshock_neg = export_distributions(sol_idiok_kshock_neg.p, prm, X, P, θg, Kg, Sg, W4_neg;
                     δ=δ, scale_m=sol_idiok_kshock_neg.m, prefix="se_kappa_neg")

# weighted quantiles of effective prices
q_effp_kshock_pos = w_quantiles(price_summary_stats_kshock_pos.incumbents.eff_price,
                     price_summary_stats_kshock_pos.incumbents.mass,
                     [0.10, 0.25, 0.50, 0.75, 0.90])
println("Effective price quantiles (κ shock pos corr): ", q_effp_kshock_pos)   

q_effp_kshock_neg = w_quantiles(price_summary_stats_kshock_neg.incumbents.eff_price,
                     price_summary_stats_kshock_neg.incumbents.mass,
                     [0.10, 0.25, 0.50, 0.75, 0.90])
println("Effective price quantiles (κ shock neg corr): ", q_effp_kshock_neg)

# changes in prices at each quantile
println("Changes in effective prices at quantiles (neg corr - pos corr): ", q_effp_kshock_neg ./ q_effp_kshock_pos)

